#!/usr/bin/env python3
"""실제 브로커/SSH/fio/TRIM 없이 네 조합, 전달 옵션, 실패·중단을 검사한다."""
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time

root = Path(__file__).resolve().parents[1]
work = Path(tempfile.mkdtemp(prefix='client42-comparison-test-'))
for file in root.glob('run_client42_*.sh'):
    shutil.copyfile(file, work / file.name)
runner = work / 'run_remote_kafka_experiments.sh'
runner.write_text('''#!/usr/bin/env bash
exec python3 "$(dirname "$0")/mock.py" "$@"
''')
(work / 'mock.py').write_text('''import json,os,sys,time
from pathlib import Path
with open(os.environ['TRACE'], 'a') as f:
    f.write(json.dumps(sys.argv[1:]) + '\\n')
if os.environ.get('HOLD'):
    Path(os.environ['TRACE']+'.pid').write_text(str(os.getpid()))
    time.sleep(30)
if os.environ.get('FAIL_SECOND') and '--delete' in sys.argv and '--no-fio' in sys.argv:
    sys.exit(7)
''')
trace = work / 'trace'
env = dict(os.environ, TRACE=str(trace))
main = ['bash', str(work / 'run_client42_comparison.sh')]

def calls():
    return [json.loads(line) for line in trace.read_text().splitlines()]

def value(args, name):
    return args[len(args) - 1 - args[::-1].index(name) + 1]

result = subprocess.run(main + ['--hz', '3', '--retry', '1', '--run-duration-minutes', '25'], env=env, capture_output=True, text=True)
assert result.returncode == 0, result.stdout + result.stderr
seen = calls()
assert len(seen) == 6
assert [(('--delete' in a), ('--fio' in a)) for a in seen] == [(False,False),(True,False),(True,False),(False,True),(True,True),(True,True)]
assert [value(a, '--deletion-mode') for a in seen if '--delete' in a] == ['BATCH','IMMEDIATE','BATCH','IMMEDIATE']
assert all('--all' not in a and value(a,'--hz') == '3' and value(a,'--retry') == '1' and value(a,'--run-duration-minutes') == '25' for a in seen)
assert len({value(a,'--output-dir') for a in seen}) == 6
for mode in ['delete_off_fio_off','delete_on_fio_off','delete_off_fio_on','delete_on_fio_on','delete_batch_fio_off','delete_immediate_fio_off','delete_batch_fio_on','delete_immediate_fio_on']:
    trace.write_text('')
    subprocess.run(['bash', str(work / f'run_client42_{mode}.sh')], env=env, check=True, stdout=subprocess.DEVNULL)
    call, = calls()
    assert '--all' in call and value(call, '--retry') == '3'
    assert ('--delete' in call) == (not mode.startswith('delete_off'))
    assert ('--fio' in call) == mode.endswith('fio_on')
    if '--delete' in call:
        assert value(call, '--deletion-mode') == ('IMMEDIATE' if 'immediate' in mode else 'BATCH')
for deletion in ['BATCH', 'IMMEDIATE']:
    trace.write_text('')
    subprocess.run(main + ['--deletion-mode', deletion], env=env, check=True, stdout=subprocess.DEVNULL)
    assert len(calls()) == 4
    assert all(value(a,'--deletion-mode') == deletion for a in calls() if '--delete' in a)
trace.write_text('')
subprocess.run(['bash', str(work / 'run_client42_delete_on_fio_off.sh'), '--deletion-mode', 'IMMEDIATE'], env=env, check=True, stdout=subprocess.DEVNULL)
assert value(calls()[0], '--deletion-mode') == 'IMMEDIATE'
trace.write_text('')
failed = subprocess.run(main, env=dict(env, FAIL_SECOND='1'), stdout=subprocess.DEVNULL)
assert failed.returncode == 7 and len(calls()) == 2
trace.write_text('')
subprocess.run(main + ['--intervals','all','--hz','2','--retry','1'], env=env, check=True, stdout=subprocess.DEVNULL)
assert len(calls()) == 24
assert [value(a,'--scan-interval-ms') for a in calls()] == [str(ms) for ms in (1000,30000,60000,300000) for _ in range(6)]
assert all(value(a,'--scan-interval-ms') == value(a,'--inactivity-check-interval-ms') for a in calls())
assert len({value(a,'--output-dir') for a in calls()}) == 24
for label, ms in [('1s',1000),('30s',30000),('1m',60000),('5m',300000)]:
    trace.write_text('')
    subprocess.run(['bash',str(work / f'run_client42_interval_{label}.sh'),'--hz','1','--retry','1'], env=env, check=True, stdout=subprocess.DEVNULL)
    assert len(calls()) == 6
    assert all(value(a,'--scan-interval-ms') == value(a,'--inactivity-check-interval-ms') == str(ms) for a in calls())
trace.write_text('')
subprocess.run(main + ['--scan-interval-ms','30000','--inactivity-check-interval-ms','60000'], env=env, check=True, stdout=subprocess.DEVNULL)
assert all(value(a,'--scan-interval-ms') == '30000' and value(a,'--inactivity-check-interval-ms') == '60000' for a in calls())
trace.write_text('')
subprocess.run(['bash',str(work / 'run_client42_intervals_1s_5m_fio1.sh'),'--hz','3','--retry','1'], env=env, check=True, stdout=subprocess.DEVNULL)
assert len(calls()) == 12
assert [value(a,'--scan-interval-ms') for a in calls()] == ['1000']*6 + ['300000']*6
assert all(value(a,'--scan-interval-ms') == value(a,'--inactivity-check-interval-ms') and value(a,'--fio-passes') == '1' for a in calls())
assert len({value(a,'--output-dir') for a in calls()}) == 12
trace.write_text('')
failed = subprocess.run(main + ['--intervals','all'], env=dict(env, FAIL_SECOND='1'), stdout=subprocess.DEVNULL)
assert failed.returncode == 7 and len(calls()) == 2
for invalid in [['--variant','invalid'], ['--hz','1','--all'], ['--no-cleanup'], ['--topic-mode','precreate'], ['--deletion-mode','bad'], ['--variant','delete_batch_fio_on','--deletion-mode','IMMEDIATE'], ['--intervals','0'], ['--intervals','all','--scan-interval-ms','1000'], ['--intervals','1000,1000'], ['--intervals','1000,'], ['--intervals','1000,500']]:
    trace.write_text('')
    result = subprocess.run(main + invalid, env=env, capture_output=True)
    assert result.returncode == 2 and not trace.read_text()
trace.write_text('')
active = subprocess.Popen(main, env=dict(env, HOLD='1'), stdout=subprocess.DEVNULL)
pidfile = Path(str(trace)+'.pid')
deadline = time.monotonic() + 10
while not pidfile.exists() and time.monotonic() < deadline:
    time.sleep(.05)
assert pidfile.exists()
blocked = subprocess.run(main, env=env, capture_output=True)
assert blocked.returncode == 2
active.send_signal(signal.SIGTERM)
assert active.wait(timeout=10) == 143 and len(calls()) == 1
try:
    os.kill(int(pidfile.read_text()), 0)
except ProcessLookupError:
    pass
else:
    raise AssertionError('child survived cancellation')
print(f'PASS: 24 full combinations; 12 selected combinations with fio passes=1; invalid lists; modes, failure, lock and cancellation. Artifacts: {work}')
