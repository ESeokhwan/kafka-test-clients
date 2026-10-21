#!/usr/bin/env python3
"""Linux integration tests: fake SSH, fake build/fio, real tmux workers and leases."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import contextlib
import time

root = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('remote_broker', root / 'tools/remote_broker.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
assert module.command(dict(project='/broker', operation='trim', args=['/data4', 'nvme1n1'])) == ['bash', '/broker/prepare_ssd_trim.sh', '/data4', 'nvme1n1']
with contextlib.nullcontext(tempfile.mkdtemp(prefix='remote-broker-test-')) as tmp:
    print('TEST_ARTIFACTS=' + tmp, flush=True)
    work = Path(tmp)
    project = work / 'project with spaces'
    (project / 'tools').mkdir(parents=True)
    shutil.copyfile(root / 'tools/remote_broker.py', project / 'tools/remote_broker.py')
    # No production build, Kafka, fio or cache drop command is used in this test.
    (project / 'gradlew').write_text('''#!/bin/bash
mkdir -p build/libs
echo MOCK_BUILD_VERBOSE_MUST_BE_DISCARDED
printf mock-jar > build/libs/apps-1.0-SNAPSHOT-all.jar
exit "${MOCK_BUILD_STATUS:-0}"
''')
    (project / 'prepare_ssd_fio.sh').write_text('#!/bin/bash\nexec sleep 60\n')
    binary = work / 'bin'
    binary.mkdir()
    ssh = binary / 'ssh'
    ssh.write_text('#!' + sys.executable + '\nimport os,sys,shlex\na=shlex.split(sys.argv[-1]); os.execvp(a[0],a)\n')
    ssh.chmod(0o755)
    env = dict(os.environ, PATH=str(binary) + ':' + os.environ['PATH'])
    cmd = [sys.executable, str(root / 'tools/remote_broker.py'), '--host', 'mock-host',
           '--project', str(project), '--run', 'test-run', '--timeout', '10']
    destination = work / 'download.jar'
    result = subprocess.run(cmd + ['--jar-destination', str(destination), 'build'], env=env, capture_output=True, text=True, timeout=50)
    assert result.returncode == 0, result.stdout + result.stderr
    assert destination.read_bytes() == b'mock-jar'
    assert 'MOCK_BUILD_VERBOSE' not in result.stdout
    logs = list((project / '.remote_jobs').glob('*/output.log'))
    assert logs and all('MOCK_BUILD_VERBOSE' not in f.read_text() for f in logs)
    print('PASS: tmux build, quoted paths, checksum download, quiet build')
    # tmux server environment does not inherit caller env; change only the fixture.
    (project / 'gradlew').write_text('#!/bin/bash\nexit 7\n')
    failed = subprocess.run(cmd + ['--jar-destination', str(destination), 'build'], env=env, capture_output=True, text=True, timeout=30)
    assert failed.returncode == 7, failed.stdout + failed.stderr
    assert destination.read_bytes() == b'mock-jar'
    print('PASS: remote failure propagated; previous local JAR preserved')
    timeout = subprocess.run(cmd + ['--timeout', '1', 'fio'], env=env, capture_output=True, text=True, timeout=15)
    assert timeout.returncode == 124, timeout.stdout + timeout.stderr
    print('PASS: bounded remote process group timeout')
    proc = subprocess.Popen(cmd + ['fio'], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(2)
    proc.send_signal(signal.SIGTERM)
    stdout, stderr = proc.communicate(timeout=15)
    assert proc.returncode == 130, stdout + stderr
    print('PASS: cancellation terminates remote worker')
    # Abrupt client death must not leave remote fio running.
    before = set((project / '.remote_jobs').glob('test-run-*'))
    proc = subprocess.Popen(cmd + ['fio'], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)
    proc.kill(); proc.wait()
    deadline = time.monotonic() + 22
    jobs = set((project / '.remote_jobs').glob('test-run-*')) - before
    assert len(jobs) == 1
    job = jobs.pop()
    while not (job / 'exit').exists() and time.monotonic() < deadline:
        time.sleep(.2)
    assert (job / 'exit').read_text() == '130'
    print('PASS: SSH/client loss cancels remote job')
