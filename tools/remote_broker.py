#!/usr/bin/env python3
"""SSH transport and bounded tmux workers; no third-party Python dependencies."""
import argparse
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
import uuid


def stamp(message):
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
    print(f'[{now:%Y-%m-%d %H:%M:%S} KST] {message}', flush=True)


def command(req):
    root = Path(req['project'])
    op = req['operation']
    if op == 'build':
        return ['bash', str(root / 'gradlew'), '-p', str(root), ':apps:shadowJar',
                '--rerun-tasks', '--no-build-cache', '--no-daemon', '--console=plain']
    if op == 'cache':
        cmd = ['sh', '-c', 'sync && printf "3\\n" > /proc/sys/vm/drop_caches']
        return cmd if os.geteuid() == 0 else ['sudo', '-n'] + cmd
    if op == 'fio':
        return ['bash', str(root / 'prepare_ssd_fio.sh')] + req['args']
    if op == 'trim':
        return ['bash', str(root / 'prepare_ssd_trim.sh')] + req['args']
    if op == 'iostat':
        return ['env', 'LC_ALL=C', 'S_TIME_FORMAT=ISO', 'iostat', '-d', '-x', '-t', '-y', '1']
    raise ValueError('Unsupported remote operation')


def stop(proc):
    if proc and proc.poll() is None:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()


def worker(directory):
    # Only this worker owns the remote child process group. Lease loss also stops it.
    import fcntl
    job = Path(directory)
    req = json.loads((job / 'request.json').read_text())
    proc = None
    status = 1
    lock = None
    previous_stdout, previous_stderr = sys.stdout, sys.stderr
    try:
        with (job / 'output.log').open('a', buffering=1) as output:
            sys.stdout = output
            sys.stderr = output
            stamp(f"START operation={req['operation']} run={req['run']} host={os.uname().nodename}")
            if req['operation'] != 'iostat':
                lock = open(Path(req['project']) / '.remote_jobs' / 'mutation.lock', 'a')
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            cmd = command(req)
            stamp('COMMAND ' + shlex.join(cmd))
            proc = subprocess.Popen(cmd, cwd=req['project'], start_new_session=True,
                                    stdout=subprocess.DEVNULL if req['operation'] == 'build' else output,
                                    stderr=subprocess.STDOUT)
            started = time.monotonic()
            while proc.poll() is None:
                if (job / 'cancel').exists() or time.time() - (job / 'lease').stat().st_mtime > 15:
                    stamp('CANCEL: request or SSH lease expired')
                    stop(proc)
                    status = 130
                    break
                if time.monotonic() - started > req['timeout']:
                    stamp('TIMEOUT')
                    stop(proc)
                    status = 124
                    break
                if req['operation'] == 'build':
                    stamp(f'BUILD progress elapsed={int(time.monotonic() - started)}s')
                time.sleep(1)
            else:
                status = proc.returncode
            if status == 0 and req['operation'] == 'build':
                import shutil
                # Immutable per-job copy prevents another build changing the download.
                shutil.copyfile(Path(req['project']) / 'build/libs/apps-1.0-SNAPSHOT-all.jar', job / 'apps.jar')
                digest = hashlib.sha256((job / 'apps.jar').read_bytes()).hexdigest()
                (job / 'sha256').write_text(digest)
            stamp(f'END exit_code={status}')
    except BaseException as exc:
        status = 1
        with (job / 'output.log').open('a') as output:
            output.write(f'WORKER ERROR: {type(exc).__name__}: {exc}\n')
        stop(proc)
    finally:
        sys.stdout, sys.stderr = previous_stdout, previous_stderr
        (job / 'exit.tmp').write_text(str(status))
        (job / 'exit.tmp').replace(job / 'exit')
        if lock:
            lock.close()
    return status


def serve(encoded):
    req = json.loads(base64.urlsafe_b64decode(encoded))
    root = Path(req['project']).resolve(strict=True)
    req['project'] = str(root)
    if not re.fullmatch(r'[a-zA-Z0-9_.-]{1,200}', req['run']):
        raise ValueError('Invalid run ID')
    if not 1 <= req['timeout'] <= 604800:
        raise ValueError('Timeout must be 1..604800 seconds')
    if req['operation'] == 'check':
        import shutil
        for name in ['tmux', 'bash', 'java'] + req['args']:
            if name == 'cache-permission':
                subprocess.run(['sudo', '-n', 'true'], check=True, timeout=10)
            elif not shutil.which(name):
                raise RuntimeError(f'Missing remote tool: {name}')
        stamp(f'REMOTE_CHECK_OK host={os.uname().nodename} project={root}')
        return 0
    command(req)  # reject unsupported operations before creating a job
    job = root / '.remote_jobs' / (req['run'] + '-' + uuid.uuid4().hex[:12])
    job.mkdir(parents=True)
    (job / 'request.json').write_text(json.dumps(req))
    (job / 'lease').touch()
    def heartbeat():
        pending = b''
        while True:
            data = os.read(sys.stdin.fileno(), 4096)
            if not data:
                break
            pending += data
            while b'\n' in pending:
                line, pending = pending.split(b'\n', 1)
                if line != b'ping':
                    (job / 'cancel').touch()
                    return
                (job / 'lease').touch()
        (job / 'cancel').touch()
    threading.Thread(target=heartbeat, daemon=True).start()
    if subprocess.run(['tmux', 'has-session', '-t', '=codex'], capture_output=True).returncode:
        subprocess.run(['tmux', 'new-session', '-d', '-s', 'codex', '-n', 'work', '-c', str(root)], check=True)
    cmd = shlex.join([sys.executable, str(Path(__file__).resolve()), 'worker', str(job)])
    subprocess.run(['tmux', 'new-window', '-d', '-t', 'codex', '-n', 'remote-' + uuid.uuid4().hex[:8],
                    '-c', str(root), cmd], check=True)
    stamp(f'REMOTE_JOB={job}')
    position = 0
    started = time.monotonic()
    try:
        while True:
            if (job / 'output.log').exists():
                with (job / 'output.log').open() as source:
                    source.seek(position)
                    data = source.read()
                    position = source.tell()
                    if data:
                        print(data, end='', flush=True)
            if (job / 'exit').exists():
                # Drain any bytes written just before completion.
                with (job / 'output.log').open() as source:
                    source.seek(position)
                    print(source.read(), end='', flush=True)
                status = int((job / 'exit').read_text())
                if status == 0 and req['operation'] == 'build':
                    print('ARTIFACT ' + json.dumps({'path': str(job / 'apps.jar'), 'sha256': (job / 'sha256').read_text()}), flush=True)
                return status
            if time.monotonic() - started > req['timeout'] + 30:
                (job / 'cancel').touch()
                return 124
            time.sleep(.2)
    finally:
        if not (job / 'exit').exists():
            (job / 'cancel').touch()


def client(argv):
    p = argparse.ArgumentParser()
    p.add_argument('--host', required=True)
    p.add_argument('--project', required=True)
    p.add_argument('--identity', default='')
    p.add_argument('--run', required=True)
    p.add_argument('--timeout', type=int, default=7200)
    p.add_argument('--jar-destination')
    p.add_argument('operation', choices=['check', 'build', 'cache', 'fio', 'trim', 'iostat'])
    p.add_argument('args', nargs=argparse.REMAINDER)
    args = p.parse_args(argv)
    if not re.fullmatch(r'[a-zA-Z0-9_][a-zA-Z0-9_.@-]*', args.host):
        p.error('Invalid SSH host')
    ssh = ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', '-o', 'ForwardAgent=no',
           '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=5', '-o', 'ServerAliveCountMax=3']
    if args.identity:
        ssh += ['-i', args.identity, '-o', 'IdentitiesOnly=yes']
    req = dict(project=args.project, operation=args.operation, run=args.run, timeout=args.timeout, args=args.args)
    token = base64.urlsafe_b64encode(json.dumps(req).encode()).decode()
    remote = shlex.join(['python3', args.project.rstrip('/') + '/tools/remote_broker.py', 'serve', token])
    proc = subprocess.Popen(ssh + [args.host, remote], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=None, text=True, start_new_session=True)
    cancelled = threading.Event()
    def cancel(*_):
        cancelled.set()
    for sig in [signal.SIGINT, signal.SIGTERM, signal.SIGHUP]:
        signal.signal(sig, cancel)
    def heartbeat():
        try:
            while proc.poll() is None:
                proc.stdin.write('cancel\n' if cancelled.is_set() else 'ping\n')
                proc.stdin.flush()
                if cancelled.wait(2):
                    proc.stdin.write('cancel\n'); proc.stdin.flush()
                    proc.wait(timeout=8)
                    return
        except (BrokenPipeError, OSError, subprocess.TimeoutExpired):
            stop(proc)
    threading.Thread(target=heartbeat, daemon=True).start()
    artifact = None
    for line in proc.stdout:
        if line.startswith('ARTIFACT '):
            artifact = json.loads(line[len('ARTIFACT '):])
        else:
            print(line, end='', flush=True)
    status = proc.wait()
    if cancelled.is_set():
        return 130
    if status:
        return status
    if args.operation == 'build':
        if not artifact or not args.jar_destination:
            raise RuntimeError('Remote build did not supply an artifact')
        destination = Path(args.jar_destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp = destination.with_name(destination.name + '.' + uuid.uuid4().hex + '.part')
        try:
            with temp.open('wb') as output:
                subprocess.run(ssh + [args.host, shlex.join(['cat', '--', artifact['path']])], stdout=output, check=True, timeout=300)
            digest = hashlib.sha256(temp.read_bytes()).hexdigest()
            if digest != artifact['sha256']:
                raise RuntimeError('Downloaded JAR checksum mismatch')
            temp.replace(destination)
            stamp(f'JAR_SYNC_OK sha256={digest} destination={destination}')
        finally:
            temp.unlink(missing_ok=True)
    return 0


if __name__ == '__main__':
    try:
        if sys.argv[1] == 'worker':
            result = worker(sys.argv[2])
        elif sys.argv[1] == 'serve':
            result = serve(sys.argv[2])
        else:
            result = client(sys.argv[1:])
        sys.exit(result)
    except Exception as exc:
        print(f'REMOTE ERROR: {type(exc).__name__}: {exc}', file=sys.stderr)
        sys.exit(1)
