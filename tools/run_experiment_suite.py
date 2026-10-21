#!/usr/bin/env python3
"""실험 순회 관리자. 원본을 보존하고 각 시도의 생산 metrics를 실행 폴더 최상단에 복사한다."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

KST = dt.timezone(dt.timedelta(hours=9))
BASE = Path(__file__).resolve().parents[1]


def now():
    return dt.datetime.now(KST).isoformat(timespec='seconds')


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError('1 이상의 정수가 필요합니다.')
    return number


def wait_before_retry(seconds, is_interrupted, log):
    """복구가 종료된 뒤 지정한 시간만큼 대기한다. 단조 시계로 시간 변경 영향을 피한다."""
    started = time.monotonic()
    deadline = started + seconds
    while not is_interrupted():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        log(f'[실패 후 재시도 대기] 경과={int(time.monotonic() - started)}초, 남은 시간={int(remaining + 0.999)}초')
        time.sleep(min(1, remaining))


def collect_metrics(root):
    """덮어쓰기 없이 원본을 복사. 실패/부분 결과도 상태와 함께 보관한다."""
    target = root
    entries = []
    for source in sorted((root / 'artifacts').glob('*.csv')):
        if not source.name.endswith(('_metric.csv', '_metrics.csv')):
            continue
        suffix = '_metrics.csv' if source.name.endswith('_metrics.csv') else '_metric.csv'
        metadata = source.with_name(source.name[:-len(suffix)] + '.run.txt')
        status = 'unknown_or_incomplete'
        if metadata.exists():
            for line in metadata.read_text(encoding='utf-8').splitlines():
                if line.startswith('FINAL_EXIT_CODE='):
                    status = line.split('=', 1)[1]
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        destination = target / source.name
        if destination.exists():
            if hashlib.sha256(destination.read_bytes()).hexdigest() != digest:
                raise FileExistsError(f'수집 파일 충돌: {destination}')
        else:
            shutil.copy2(source, destination)
        entries.append(dict(file=source.name, source=str(source), metadata=str(metadata),
                            final_exit_code=status, sha256=digest))
    temporary = target / 'metrics_manifest.json.tmp'
    temporary.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding='utf-8')
    temporary.replace(target / 'metrics_manifest.json')
    return len(entries)


def parser():
    p = argparse.ArgumentParser(description='42→11 실험: 전체 순회, 실패 시 1회 재시도, CSV 수집')
    p.add_argument('--hz-list', nargs='+', type=positive, default=[3, 4, 5])
    p.add_argument('--rounds', type=positive, default=2)
    p.add_argument('--intervals', nargs='+', type=positive, default=[1000, 300000])
    p.add_argument('--delete-modes', nargs='+', choices=['BATCH', 'IMMEDIATE'], default=['BATCH', 'IMMEDIATE'])
    p.add_argument('--scenario-set', choices=['auto-delete', 'precreate-auto'], default='auto-delete',
                   help='precreate-auto: PRECREATE/OFF, AUTO/OFF, AUTO/선택한 삭제 방식')
    trim = p.add_mutually_exclusive_group()
    trim.add_argument('--fstrim', dest='fstrim', action='store_true')
    trim.add_argument('--no-fstrim', dest='fstrim', action='store_false')
    p.set_defaults(fstrim=False)
    p.add_argument('--fstrim-target-dir', default='/data4')
    p.add_argument('--fstrim-wait-seconds', type=positive, default=600)
    p.add_argument('--expected-disk', default='nvme1n1')
    p.add_argument('--topic-prepare-timeout-seconds', type=positive, default=600)
    p.add_argument('--fio-passes', type=int, choices=range(0, 101), default=0)
    p.add_argument('--run-duration-minutes', type=positive, default=60)
    p.add_argument('--failure-retries', type=int, choices=[0, 1], default=1)
    p.add_argument('--retry-delay-seconds', type=int, default=60)
    p.add_argument('--step-delay-seconds', type=int, default=10)
    p.add_argument('--inter-experiment-delay-seconds', type=int, default=60)
    p.add_argument('--cleanup-timeout-seconds', type=positive, default=300)
    p.add_argument('--iostat', action='store_true', help='브로커의 디스크 상태를 1초 간격으로 수집')
    p.add_argument('--require-cleanup-success', action='store_true',
                   help='실패 후 정리 완료 기록이 없으면 재시도와 다음 실험을 중단')
    p.add_argument('--output-dir', type=Path, default=BASE / 'experiment_results')
    p.add_argument('--dry-run', action='store_true', help='서버 접속 없이 실행 계획만 기록')
    return p


def main():
    args = parser().parse_args()
    if args.cleanup_timeout_seconds > 86400:
        raise SystemExit('cleanup 제한 시간은 86400초 이하입니다.')
    if args.fstrim_wait_seconds < 600:
        raise SystemExit('fstrim 대기는 최소 600초입니다.')
    if max(args.hz_list) > 20 or min(args.retry_delay_seconds, args.step_delay_seconds,
                                    args.inter_experiment_delay_seconds) < 0:
        raise SystemExit('Hz는 1~20, 대기시간은 0 이상이어야 합니다.')
    for values in (args.hz_list, args.intervals, args.delete_modes):
        if len(values) != len(set(values)):
            raise SystemExit('중복 조건은 허용하지 않습니다.')
    # 같은 실행기의 동시 실행을 막는다. 다른 진입점의 실험은 운영자가 확인한다.
    lock = None
    if not args.dry_run:
        import fcntl
        lock = (BASE / '.client42_comparison.lock').open('a')
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit('다른 비교 실험이 실행 중입니다.')
    stamp = dt.datetime.now(KST).strftime('%Y%m%d_%H%M%S')
    suite_id = f'{stamp}_{os.getpid()}'
    root = args.output_dir.resolve() / f'suite_{suite_id}_KST'
    artifacts = root / 'artifacts'
    artifacts.mkdir(parents=True, exist_ok=False)
    (root / 'settings.json').write_text(json.dumps(vars(args), default=str, ensure_ascii=False, indent=2), encoding='utf-8')
    child = None
    interrupted = 0
    failures = 0

    def log(message):
        line = f'[{now()}] {message}'
        print(line, flush=True)
        with (root / 'suite.log').open('a', encoding='utf-8') as handle:
            handle.write(line + '\n')

    def stop(signum, _frame):
        nonlocal interrupted
        interrupted = 128 + signum
        if child is not None and child.poll() is None:
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

    for sig in (signal.SIGINT, signal.SIGTERM, *([signal.SIGHUP] if hasattr(signal, 'SIGHUP') else [])):
        signal.signal(sig, stop)
    try:
        case_number = 0
        for round_number in range(1, args.rounds + 1):
            for hz in args.hz_list:
                scenarios = [('auto', mode) for mode in args.delete_modes]
                if args.scenario_set == 'precreate-auto':
                    scenarios = [('precreate', 'OFF'), ('auto', 'OFF')] + scenarios
                for topic_mode, mode in scenarios:
                    # 삭제 OFF에서는 검사기를 실행하지 않으므로 간격 비교를 중복하지 않는다.
                    for interval in (args.intervals if mode != 'OFF' else [1000]):
                        case_number += 1
                        for attempt in range(1, args.failure_retries + 2):
                            if interrupted:
                                return interrupted
                            label = f'r{round_number}_c{case_number}_try{attempt}'
                            # 각 시도의 소유 범위를 분리하여 실패 잔여 토픽과 충돌하지 않는다.
                            prefix = f'test_creator_{suite_id}_{label}'
                            command = ['bash', str(BASE / 'run_remote_kafka_experiments.sh'),
                                       '--hz', str(hz), '--retry', '1', '--topic-mode', topic_mode,
                                       '--topic-prepare-timeout-seconds', str(args.topic_prepare_timeout_seconds),
                                       '--output-layout', 'flat', '--experiment-label', label,
                                       '--no-build', '--no-drop-caches', '--iostat' if args.iostat else '--no-iostat',
                                       '--cleanup-timeout-seconds', str(args.cleanup_timeout_seconds),
                                       '--fstrim' if args.fstrim else '--no-fstrim',
                                       '--fstrim-target-dir', args.fstrim_target_dir,
                                       '--fstrim-wait-seconds', str(args.fstrim_wait_seconds),
                                       '--expected-disk', args.expected_disk,
                                       '--stats-enabled', '--cleanup',
                                       '--no-delete' if mode == 'OFF' else '--delete',
                                       '--deletion-mode', 'BATCH' if mode == 'OFF' else mode,
                                       '--brokers', '166.104.110.11:9092', '--prefix', prefix,
                                       '--delete-topic-prefix', prefix + '_',
                                       '--run-duration-minutes', str(args.run_duration_minutes),
                                       '--topic-cnt', '-1', '--topic-lifetime-ms', '40000',
                                       '--produce-hz-per-topic', '100', '--msg-size', '4096',
                                       '--start-index', '0', '--start-barrier-delay-ms', '10000',
                                       '--inactivity-threshold-ms', '600000', '--scan-log-interval-ms', '1000',
                                       '--scan-interval-ms', str(interval), '--inactivity-check-interval-ms', str(interval),
                                       '--step-delay-seconds', str(args.step_delay_seconds),
                                       '--inter-experiment-delay-seconds', str(args.inter_experiment_delay_seconds),
                                       '--remote-stage-timeout-seconds', '28800', '--output-dir', str(artifacts)]
                            if args.fio_passes:
                                command += ['--fio', '--fio-passes', str(args.fio_passes), '--fio-target-dir', '/data4',
                                            '--fio-size-gib', 'auto', '--reserve-gib', '2']
                            else:
                                command += ['--no-fio']
                            record = dict(label=label, round=round_number, hz=hz, topic_mode=topic_mode, delete_mode=mode,
                                          interval_ms=interval if mode != 'OFF' else None, attempt=attempt, prefix=prefix,
                                          start_kst=now(), command=command, dry_run=args.dry_run)
                            log(f'{label}: {hz}Hz {topic_mode} delete={mode} 검사={interval if mode != "OFF" else "N/A"}ms fio={args.fio_passes} fstrim={args.fstrim} 시작')
                            # 실행 전에도 명령을 남겨 비정상 종료 시 추적 가능하게 한다.
                            with (root / 'commands.jsonl').open('a', encoding='utf-8') as handle:
                                handle.write(json.dumps(record, ensure_ascii=False) + '\n')
                            status = 0
                            if not args.dry_run:
                                console_tag = f'{label}_{hz}Hz_{topic_mode}_fio{args.fio_passes}_del-{mode.lower()}_scan{interval}_check{interval}_{args.run_duration_minutes}min'
                                started_stamp = dt.datetime.now(KST).strftime('%Y%m%d_%H%M%S')
                                console_path = artifacts / f'{console_tag}_start_{started_stamp}_KST_end_PENDING_console.log'
                                child = subprocess.Popen(command, start_new_session=True,
                                                         env=dict(os.environ, EXPERIMENT_CLASS_DIR=str(root / '.runtime')),
                                                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                                         text=True, encoding='utf-8', errors='replace')
                                with console_path.open('w', encoding='utf-8') as handle:
                                    for line in child.stdout:
                                        stamped = f'[{now()}] {line}'
                                        print(stamped, end='', flush=True)
                                        handle.write(stamped)
                                        handle.flush()
                                status = child.wait()
                                child = None
                                ended_stamp = dt.datetime.now(KST).strftime('%Y%m%d_%H%M%S')
                                final_console = console_path.with_name(console_path.name.replace('end_PENDING', f'end_{ended_stamp}_KST'))
                                console_path.rename(final_console)
                                record['console_log'] = str(final_console)
                            record.update(end_kst=now(), exit_code=status)
                            with (root / 'attempts.jsonl').open('a', encoding='utf-8') as handle:
                                handle.write(json.dumps(record, ensure_ascii=False) + '\n')
                            log(f'{label}: 종료 코드={status}')
                            # 전체 순회가 끝나기 전에도 종료된 시도의 CSV를 바로 찾을 수 있게 한다.
                            collect_metrics(root)
                            if interrupted or status in (129, 130, 143, -1, -2, -15):
                                return interrupted or (status if status > 0 else 128 - status)
                            if status == 0:
                                break
                            if args.require_cleanup_success:
                                records = list(artifacts.glob(f'{label}_*.run.txt'))
                                safe = len(records) == 1 and any(
                                    line.startswith('EVENT ') and 'status=FAILED_CLEANED next_experiment_allowed=true' in line
                                    for line in records[0].read_text(encoding='utf-8').splitlines())
                                if not safe:
                                    log('정리 완료를 확인할 수 없어 전체 중단합니다. 잔존 토픽 및 실패 기록을 확인하세요.')
                                    return 1
                            if attempt <= args.failure_retries:
                                log('실패한 조합을 1회 재시도합니다. cleanup 실패 시 잔여 상태가 다음 측정에 영향을 줄 수 있습니다.')
                                wait_before_retry(args.retry_delay_seconds, lambda: interrupted, log)
                            else:
                                failures += 1
                                log('재시도 소진: 실패를 기록하고 다음 조합으로 진행합니다.')
        log(f'전체 순회 완료: 최종 실패 조합={failures}')
        return 1 if failures else 0
    finally:
        count = collect_metrics(root)
        log(f'생산 CSV {count}개 수집: {root} (실패/부분 결과는 metrics_manifest.json 확인)')
        if lock:
            lock.close()


if __name__ == '__main__':
    raise SystemExit(main())
