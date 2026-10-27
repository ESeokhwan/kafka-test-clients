#!/usr/bin/env bash
# 실제 Kafka/SSD에 접근하지 않는 통합 실행 순서 테스트. Linux 세션 종료는 별도 서버 검증 필요.
set -Eeuo pipefail
shopt -s globstar
root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"
export TRACE="$work/trace"
export BUILD_TRACE="$work/build-trace"
# setsid와 음수 PID 신호를 모의 처리하여 Git Bash에서도 제어 흐름을 검사한다.
setsid() { "$@"; }
kill() {
    local signal="$1" pid="${@: -1}"
    builtin kill "$signal" "${pid#-}" 2>/dev/null
}
javac() { :; }
# 캐시 작업도 모의 처리한다. root로 테스트해도 /proc에는 쓰지 않는다.
uname() { echo Linux; }
sh() {
    [[ "$#" == 2 && "$1" == -c && "$2" == 'sync && printf "3\n" > /proc/sys/vm/drop_caches' ]] || return 31
    echo drop_caches >> "$TRACE"
    [[ "${FAIL_CACHE_DROP:-false}" != true ]] || return 32
}
sudo() {
    [[ "$1" == -n ]] || return 33
    shift
    "$@"
}
iostat() {
    [[ "$*" == '-d -x -t -y 1' ]] || return 13
    echo 'MOCK_IOSTAT interval=1'
    while :; do command sleep 0.1; done
}
java() {
    if [[ " $* " == *' JmxPreflight '* ]]; then
        echo jmx_check >> "$TRACE"
        [[ "${FAIL_JMX_PREFLIGHT:-false}" != true ]] || { echo 'Connection refused to host: 127.0.1.1' >&2; return 41; }
        echo '[JMX Preflight] READY'
    elif [[ "${CLEANUP_ONCE:-false}" == true ]]; then
        echo cleanup >> "$TRACE"
        echo '[Cleanup] phase=VERIFY initialTargets=2 remaining=1'
        command sleep "${CLEANUP_TEST_SECONDS:-0}"
        [[ "${FAIL_CLEANUP:-false}" != true ]] || return 7
    else
        [[ "$DELETE_TOPIC_PREFIX" == test_creator_ ]] || return 12
        echo delete >> "$TRACE"
        echo '[Daemon] READY'
        [[ "${FAIL_MONITOR:-false}" != true ]] || return 8
        # 종료 신호까지 기다리며 짧은 대기만 사용하는 모의 데몬이다.
        while :; do command sleep 0.1; done
    fi
}
bash() {
    local file="$1" arg metrics=""
    shift
    if [[ "$file" == */gradlew ]]; then
        [[ " $* " == *' :apps:shadowJar --rerun-tasks --no-build-cache --no-daemon --console=plain '* ]] || return 21
        echo build >> "$BUILD_TRACE"
        [[ "${FAIL_BUILD:-false}" != true ]] || return 22
        echo 'MOCK_BUILD_VERBOSE_SHOULD_NOT_APPEAR'
    elif [[ "$file" == *prepare_ssd_trim.sh ]]; then
        [[ "$*" == '/data4 nvme1n1' ]] || return 34
        echo trim >> "$TRACE"
        [[ "${FAIL_TRIM:-false}" != true ]] || return 35
    elif [[ "$file" == *prepare_ssd_fio.sh ]]; then
        echo fio >> "$TRACE"
        [[ "${FAIL_FIO:-false}" != true ]] || return 9
    elif [[ "${1:-}" == apps && "${2:-}" == BrokerExtension ]]; then
        echo 'MOCK_ROLLOUT_COMPLETE'
        return 0
    else
        if [[ " $* " == *' --check-topics-absent '* ]]; then
            [[ "${FAIL_TOPIC_CHECK:-false}" != true ]] || return 23
            return 0
        fi
        if [[ " $* " == *' --prepare-topics-only '* ]]; then
            echo precreate >> "$TRACE"
            [[ "${FAIL_PRECREATE:-false}" != true ]] || return 18
            echo 'Topic preparation complete'
            return 0
        fi
        echo producer >> "$TRACE"
        if [[ "${FAIL_PRODUCER_ONCE:-false}" == true && ! -f "$TRACE.producer-failed" ]]; then
            touch "$TRACE.producer-failed"
            echo 'MOCK_PRODUCER failure' >&2
            return 17
        fi
        echo 'MOCK_PRODUCER running'
        command sleep "${PRODUCER_TEST_SECONDS:-0}"
        while [[ $# -gt 0 ]]; do
            arg="$1"; shift
            if [[ "$arg" == --metrics-file ]]; then metrics="$1"; shift; fi
        done
        printf 'mock,csv\n1,2\n' > "$metrics"
    fi
}
# 각 테스트는 독립된 Bash 셸에서 실행하여 실패 시 EXIT 트랩도 검증한다.
export -f setsid kill javac java bash iostat
export -f uname sh sudo
export FIO_SCRIPT="$root/prepare_ssd_fio.sh" DAEMON_SOURCE="$root/KafkaJmxProducerBatchDaemon.java"
export RUNNER="$root/scripts/kafka_clients_run.sh"
if [[ ! -f "$RUNNER" ]]; then
    export RUNNER="$root/kafka-test-clients_extracted/kafka-test-clients/scripts/kafka_clients_run.sh"
fi
export OUTPUT_DIR="$work/output" INTER_EXPERIMENT_DELAY_SECONDS=0 STEP_DELAY_SECONDS=0 DROP_CACHES=false BUILD_ENABLED=true FSTRIM_ENABLED=false
# command -v는 위 모의 함수를 반환하므로 실행 파일 경로를 직접 조회한다.
real_bash="$(type -P bash)"
"$real_bash" "$root/run_kafka_experiments.sh" --all --delete --cleanup >"$work/initial.stdout"
if grep -q 'MOCK_BUILD_VERBOSE_SHOULD_NOT_APPEAR' "$work/initial.stdout" "$OUTPUT_DIR"/**/*_build.log; then exit 1; fi
[[ "$(wc -l < "$BUILD_TRACE")" -eq 1 ]] || exit 1
actual="$(tr '\n' ' ' < "$TRACE")"
expected='jmx_check fio delete producer cleanup jmx_check fio delete producer cleanup jmx_check fio delete producer cleanup jmx_check fio delete producer cleanup jmx_check fio delete producer cleanup '
[[ "$actual" == "$expected" ]] || { echo "순서 오류: $actual"; exit 1; }
# JMX 연결 실패는 첫 회차에서 전체 중단하고 TRIM/fio/cache/삭제/생산을 시작하지 않는다.
printf '' > "$TRACE"
if FAIL_JMX_PREFLIGHT=true "$real_bash" "$root/run_kafka_experiments.sh" --all --delete --fio --fstrim --drop-caches --no-iostat --output-dir "$work/jmx-fail" >/dev/null 2>&1; then exit 1; fi
[[ "$(tr '\n' ' ' < "$TRACE")" == 'jmx_check ' ]] || exit 1
grep -q 'status=PREFLIGHT_FAILED cleanup_skipped=true' "$work/jmx-fail"/**/*.run.txt
printf '' > "$TRACE"
"$real_bash" "$root/run_kafka_experiments.sh" --hz=3 --no-fio --no-delete --cleanup >/dev/null
[[ "$(tr '\n' ' ' < "$TRACE")" == 'producer cleanup ' ]] || exit 1
printf '' > "$TRACE"
if FAIL_FIO=true "$real_bash" "$root/run_kafka_experiments.sh" 3 >/dev/null 2>&1; then exit 1; fi
[[ "$(tr '\n' ' ' < "$TRACE")" == 'fio cleanup ' ]] || exit 1
printf '' > "$TRACE"
if FAIL_CLEANUP=true "$real_bash" "$root/run_kafka_experiments.sh" --all --cleanup >/dev/null 2>&1; then exit 1; fi
[[ "$(tr '\n' ' ' < "$TRACE")" == 'fio producer cleanup cleanup ' ]] || exit 1
printf '' > "$TRACE"
FIO_ENABLED=false "$real_bash" "$root/run_kafka_experiments.sh" --mode fio --fio-passes 5 --reserve-gib=4 >/dev/null
[[ "$(cat "$TRACE")" == fio ]] || exit 1
printf '' > "$TRACE"
"$real_bash" "$root/run_kafka_experiments.sh" --mode=cleanup --cleanup-topic-prefix test_scope_ >/dev/null
[[ "$(cat "$TRACE")" == cleanup ]] || exit 1
# 생성된 생산 CSV가 정확한 명령과 설정 기록으로 연결되는지 검사한다.
for csv in "$OUTPUT_DIR"/**/*_metrics.csv; do
    record="${csv%_metrics.csv}.run.txt"
    [[ -s "$record" ]] || { echo "기록 누락: $csv"; exit 1; }
    grep -q '^PRODUCER_COMMAND=' "$record"
    grep -q '^ORIGINAL_COMMAND=' "$record"
    grep -q '^INACTIVITY_THRESHOLD_MS=600000$' "$record"
    grep -q 'status=SUCCESS\|status=FAILED' "$record"
done
# fio 실패에도 시작 시 기록된 설정과 종료 상태가 보존되어야 한다.
grep -l 'stage=FIO' "$OUTPUT_DIR"/**/*.run.txt | while IFS= read -r record; do
    grep -q 'status=SUCCESS\|status=FAILED' "$record"
done
[[ -n "$(find "$OUTPUT_DIR" -maxdepth 1 -name '*_runs.tsv' -print -quit)" ]]
# 기본 활성화된 iostat의 인수와 로그 연결을 검증한다.
for log in "$OUTPUT_DIR"/**/*_iostat.log; do
    grep -q 'MOCK_IOSTAT interval=1' "$log"
done
grep -q 'IOSTAT_COMMAND=' "$OUTPUT_DIR"/**/*.run.txt
# 단일 실험도 정리하며, 정리 2초는 대기 구간 4초에 포함된다 (합계 6초가 아님).
CLEANUP_TEST_SECONDS=2 "$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --no-fio --no-delete --no-iostat --cleanup --inter-experiment-delay-seconds 4 --output-dir "$work/timing" >/dev/null
elapsed="$(sed -n 's/.*stage=COOLDOWN_END elapsed_seconds=//p' "$work/timing"/**/*.run.txt)"
[[ "$elapsed" -ge 4 && "$elapsed" -lt 6 ]] || { echo "잘못된 정리·대기 시간: $elapsed"; exit 1; }
# 정리가 딜레이를 초과하면 중간에 끊지 않고 완료까지 기다린다.
CLEANUP_TEST_SECONDS=2 "$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --no-fio --no-delete --no-iostat --cleanup --inter-experiment-delay-seconds 1 --output-dir "$work/overrun" >/dev/null
elapsed="$(sed -n 's/.*stage=COOLDOWN_END elapsed_seconds=//p' "$work/overrun"/**/*.run.txt)"
[[ "$elapsed" -ge 2 ]] || exit 1
# 생산·삭제 상태를 동시에 표시하고 기록 파일에도 보존하는지 검사한다.
PRODUCER_TEST_SECONDS=6 CLEANUP_TEST_SECONDS=2 "$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --no-fio --delete --no-iostat --cleanup --inter-experiment-delay-seconds 4 --output-dir "$work/progress" >"$work/progress.stdout"
for pattern in '[STEP 2 완료]' '[STEP 2 삭제기 상태]' '[STEP 3 생산 진행] 실행 중:' '남은 시간=' '[STEP 3 생산 진행] 작업 종료:' '[STEP 5 cleanup 진행] 실행 중:' 'phase=VERIFY' '[STEP 5 대기]'; do
    grep -qF "$pattern" "$work/progress.stdout"
    grep -qF "$pattern" "$work/progress"/**/*.run.txt
done
if grep -E '\[STEP 3 생산 진행\].*(PID=|최근 기록=)' "$work/progress.stdout"; then exit 1; fi
grep -qF 'MOCK_PRODUCER running' "$work/progress"/**/*_producer.log
# 삭제 OFF도 생산 상태를 표시하며, 삭제 상태는 출력하지 않는다.
PRODUCER_TEST_SECONDS=3 "$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --run-duration-minutes 0.01 --no-fio --no-delete --no-iostat --cleanup --output-dir "$work/progress-off" >"$work/progress-off.stdout"
grep -qE '\[STEP 3 생산 진행\] 실행 중: 경과=[0-9]+초, 남은 시간=0초$' "$work/progress-off.stdout"
if grep -q '남은 시간=-' "$work/progress-off.stdout"; then exit 1; fi
grep -qF '[STEP 3 생산 진행] 실행 중:' "$work/progress-off.stdout"
if grep -qF '[STEP 2 삭제기 상태]' "$work/progress-off.stdout"; then exit 1; fi
# 각 Hz를 연속 2회 실행하고 결과와 반복 메타데이터를 분리한다.
build_before="$(wc -l < "$BUILD_TRACE")"
"$real_bash" "$root/run_kafka_experiments.sh" --all --retry=2 --no-fio --no-delete --no-iostat --cleanup --output-dir "$work/repeat-all" >/dev/null
[[ "$(wc -l < "$BUILD_TRACE")" -eq $((build_before + 1)) ]] || exit 1
awk -F '\t' 'NR>1 {i=NR-2; if ($2 != int(i/2)+1 || $7 != i%2+1 || $8 != 2) exit 1} END {if (NR != 11) exit 1}' "$work/repeat-all"/**/*_runs.tsv
[[ "$(find "$work/repeat-all" -name '*_metrics.csv' | wc -l)" -eq 10 ]]
for record in "$work/repeat-all"/**/*.run.txt; do
    grep -q '^RETRY=2$' "$record"
    grep -q '^RETRY_INDEX=[12]$' "$record"
    grep -q '^EXPERIMENT_PREFIX=test_creator$' "$record"
    grep -q 'CLEANUP_TOPIC_PREFIX=test_creator_0_ CLEANUP_START_INDEX=0 CLEANUP_TOPIC_COUNT=' "$record"
done
# 같은 Hz에서도 매번 fio와 cleanup을 수행한다.
printf '' > "$TRACE"
"$real_bash" "$root/run_kafka_experiments.sh" --hz 3 --retry 2 --fio --no-delete --no-iostat --cleanup --output-dir "$work/repeat-single" >/dev/null
[[ "$(tr '\n' ' ' < "$TRACE")" == 'fio producer cleanup fio producer cleanup ' ]]
# 복구 cleanup도 실패하면 다음 반복을 실행하지 않는다.
printf '' > "$TRACE"
if FAIL_CLEANUP=true "$real_bash" "$root/run_kafka_experiments.sh" --hz 3 --retry 2 --no-fio --no-delete --no-iostat --cleanup --output-dir "$work/repeat-fail" >/dev/null 2>&1; then exit 1; fi
[[ "$(tr '\n' ' ' < "$TRACE")" == 'producer cleanup cleanup ' ]]
grep -q 'status=RECOVERY_FAILED' "$work/repeat-fail"/**/*.run.txt
# 실패 시 cleanup 후 다음 반복을 실행하고 실패를 최종 종료 코드로 알린다.
printf '' > "$TRACE"
if FAIL_PRODUCER_ONCE=true "$real_bash" "$root/run_kafka_experiments.sh" --hz 3 --retry 2 --no-fio --no-delete --no-iostat --cleanup --output-dir "$work/recover" >/dev/null 2>&1; then exit 1; fi
[[ "$(tr '\n' ' ' < "$TRACE")" == 'producer cleanup producer cleanup ' ]]
grep -q 'status=FAILED_CLEANED' "$work/recover"/**/*repeat1*.run.txt
grep -q 'status=SUCCESS' "$work/recover"/**/*repeat2*.run.txt
grep -q 'MOCK_PRODUCER failure' "$work/recover"/**/*repeat1*_producer.log
grep -q 'FAILED exit_code=' "$work/recover"/**/*repeat1*_failure.log
[[ -f "$(find "$work/recover" -name '*_recovery_cleanup.log' -print -quit)" ]]
# 잘못된 횟수와 반복 미지원 모드는 작업 시작 전에 거부한다.
for invalid in 0 -1 1.5 abc 10000; do
    if "$real_bash" "$root/run_kafka_experiments.sh" --retry "$invalid" >/dev/null 2>&1; then exit 1; fi
done
if "$real_bash" "$root/run_kafka_experiments.sh" --mode fio --retry 2 >/dev/null 2>&1; then exit 1; fi
# 사전 생성은 매 반복 fio보다 앞서 실행되며 생산 전에 완료되어야 한다.
printf '' > "$TRACE"
"$real_bash" "$root/run_kafka_experiments.sh" --hz 2 --retry 2 --topic-mode precreate --fio --no-delete --no-iostat --cleanup --output-dir "$work/precreate-fio" >/dev/null
[[ "$(tr '\n' ' ' < "$TRACE")" == 'cleanup precreate fio producer cleanup cleanup precreate fio producer cleanup ' ]]
grep -q 'PRODUCER_COMMAND=.*--require-existing-topics' "$work/precreate-fio"/**/*.run.txt
printf '' > "$TRACE"
"$real_bash" "$root/run_kafka_experiments.sh" --hz 2 --run-duration-minutes 20 --msg-size 2048 --topic-mode precreate --no-fio --no-delete --no-iostat --cleanup --output-dir "$work/precreate-only" >/dev/null
[[ "$(tr '\n' ' ' < "$TRACE")" == 'cleanup precreate producer cleanup ' ]]
grep -q 'stage=PRECREATE_CLEANUP_SUCCESS' "$work/precreate-only"/**/*.run.txt
grep -q 'CLEANUP_START_INDEX=0 CLEANUP_TOPIC_COUNT=2400' "$work/precreate-only"/**/*.run.txt
[[ -s "$(find "$work/precreate-only" -name '*_before_precreate_cleanup.log' -print -quit)" ]]
[[ "$(find "$work/precreate-only" -mindepth 1 -maxdepth 1 -type d -name '*2Hz_mode-precreate_fio-off_delete-off_20min_start_*' | wc -l)" -eq 1 ]]
grep -q 'TOPIC_PRECREATE_COMMAND=.*--msg-size 2048' "$work/precreate-only"/**/*.run.txt
grep -q '^PRECREATE_SEED_MESSAGES_PER_TOPIC=1' "$work/precreate-only"/**/*.run.txt
[[ "$(find "$work/precreate-fio" -mindepth 1 -maxdepth 1 -type d -name '*mode-precreate_fio-on_delete-off_*' | wc -l)" -eq 2 ]]
printf '' > "$TRACE"
if FAIL_CLEANUP=true "$real_bash" "$root/run_kafka_experiments.sh" --all --topic-mode precreate --no-fio --no-delete --no-iostat --cleanup --output-dir "$work/precleanup-fail" >/dev/null 2>&1; then exit 1; fi
[[ "$(tr '\n' ' ' < "$TRACE")" == 'cleanup ' ]]
printf '' > "$TRACE"
if FAIL_TOPIC_CHECK=true "$real_bash" "$root/run_kafka_experiments.sh" --all --topic-mode precreate --no-fio --no-delete --no-iostat --cleanup --output-dir "$work/precleanup-residual" >/dev/null 2>&1; then exit 1; fi
[[ "$(tr '\n' ' ' < "$TRACE")" == 'cleanup ' ]]
printf '' > "$TRACE"
if FAIL_PRECREATE=true "$real_bash" "$root/run_kafka_experiments.sh" --hz 2 --topic-mode precreate --fio --no-delete --no-iostat --cleanup --output-dir "$work/precreate-fail" >/dev/null 2>&1; then exit 1; fi
[[ "$(tr '\n' ' ' < "$TRACE")" == 'cleanup precreate cleanup ' ]]
if "$real_bash" "$root/run_kafka_experiments.sh" --topic-mode precreate --delete >/dev/null 2>&1; then exit 1; fi
if "$real_bash" "$root/run_kafka_experiments.sh" --topic-mode precreate --no-cleanup >/dev/null 2>&1; then exit 1; fi
# 빌드 실패 시 기존 JAR이 있어도 fio/토픽 생성/cleanup 등 실험을 시작하지 않는다.
printf '' > "$TRACE"
if FAIL_BUILD=true "$real_bash" "$root/run_kafka_experiments.sh" --all --retry 2 --output-dir "$work/build-fail" >/dev/null 2>&1; then exit 1; fi
[[ ! -s "$TRACE" ]]
grep -q 'status=EXIT exit_code=1' "$work/build-fail"/**/*_build.txt
grep -q 'BUILD_LOG=' "$work/repeat-all"/**/*.run.txt
printf '' > "$TRACE"
if FAIL_TOPIC_CHECK=true "$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --retry 2 --output-dir "$work/collision" >/dev/null 2>&1; then exit 1; fi
[[ ! -s "$TRACE" ]]
grep -q 'status=PREFLIGHT_FAILED cleanup_skipped=true' "$work/collision"/**/*.run.txt
printf '' > "$TRACE"
"$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --topic-mode precreate --no-fio --no-delete --no-iostat --drop-caches --step-delay-seconds 1 --output-dir "$work/cache-success" >/dev/null
[[ "$(tr '\n' ' ' < "$TRACE")" == 'drop_caches cleanup precreate producer cleanup drop_caches ' ]]
grep -q '^DROP_CACHES=true' "$work/cache-success"/**/*.run.txt
grep -q '^STEP_DELAY_SECONDS=1' "$work/cache-success"/**/*.run.txt
grep -q 'stage=STEP_DELAY from=TOPIC_PRECREATE to=PRODUCER requested_seconds=1' "$work/cache-success"/**/*.run.txt
grep -q '남은 시간=1초' "$work/cache-success"/**/*.run.txt
printf '' > "$TRACE"
(unset DROP_CACHES; "$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --no-fio --no-iostat --output-dir "$work/cache-default" >/dev/null)
[[ "$(tr '\n' ' ' < "$TRACE")" == 'drop_caches producer cleanup drop_caches ' ]]
grep -q '^DROP_CACHES=true' "$work/cache-default"/**/*.run.txt
printf '' > "$TRACE"
(unset DROP_CACHES; "$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --no-fio --no-iostat --no-drop-caches --output-dir "$work/cache-default-off" >/dev/null)
[[ "$(tr '\n' ' ' < "$TRACE")" == 'producer cleanup ' ]]
grep -q '^DROP_CACHES=false' "$work/cache-default-off"/**/*.run.txt
printf '' > "$TRACE"
if FAIL_FIO=true "$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --drop-caches --no-iostat --output-dir "$work/cache-recovery" >/dev/null 2>&1; then exit 1; fi
[[ "$(tr '\n' ' ' < "$TRACE")" == 'drop_caches fio cleanup drop_caches ' ]]
printf '' > "$TRACE"
if FAIL_CACHE_DROP=true "$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --drop-caches --no-iostat --output-dir "$work/cache-fail" >/dev/null 2>&1; then exit 1; fi
[[ "$(tr '\n' ' ' < "$TRACE")" == 'drop_caches ' ]]
printf '' > "$TRACE"
"$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --no-fio --no-iostat --drop-caches --no-drop-caches --output-dir "$work/cache-off" >/dev/null
[[ "$(tr '\n' ' ' < "$TRACE")" == 'producer cleanup ' ]]
if "$real_bash" "$root/run_kafka_experiments.sh" --step-delay-seconds -1 >/dev/null 2>&1; then exit 1; fi
printf '' > "$TRACE"
builds_before=$(wc -l < "$BUILD_TRACE")
FAIL_BUILD=true "$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --no-build --no-fio --no-iostat --output-dir "$work/no-build" >/dev/null
[[ "$(wc -l < "$BUILD_TRACE")" == "$builds_before" ]]
[[ "$(tr '\n' ' ' < "$TRACE")" == 'producer cleanup ' ]]
grep -q '^BUILD_ENABLED=false' "$work/no-build"/**/*.run.txt
grep -q 'stage=BUILD status=SKIPPED' "$work/no-build"/**/*_build.txt
# Gradle 프로젝트 파일이 없어도 기존 JAR만 있으면 빌드를 생략할 수 있다.
mkdir -p "$work/jar-only/scripts" "$work/jar-only/build/libs"
cp "$RUNNER" "$work/jar-only/scripts/kafka_clients_run.sh"
printf '' > "$TRACE"
if "$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --no-build --client-dir "$work/jar-only" --no-iostat --output-dir "$work/missing-jar" >/dev/null 2>&1; then exit 1; fi
[[ ! -s "$TRACE" ]]
project_dir="$(cd "$(dirname "$RUNNER")/.." && pwd)"
cp "$project_dir/build/libs/apps-1.0-SNAPSHOT-all.jar" "$work/jar-only/build/libs/"
"$real_bash" "$root/run_kafka_experiments.sh" --mode fio --no-build --client-dir "$work/jar-only" --no-iostat --output-dir "$work/jar-only-run" >/dev/null
[[ "$(cat "$TRACE")" == fio ]]
printf '' > "$TRACE"
if FAIL_BUILD=true "$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --no-build --build --output-dir "$work/build-override" >/dev/null 2>&1; then exit 1; fi
[[ ! -s "$TRACE" ]]
# 각 반복은 독립 폴더를 사용하고 최종 파일명 및 메타데이터에 시작/종료 시각을 남긴다.
[[ "$(find "$work/repeat-all" -mindepth 1 -maxdepth 1 -type d -name '*_start_*_KST' | wc -l)" -eq 10 ]]
[[ -z "$(find "$work/repeat-all" "$work/recover" "$work/collision" -name '*_end_PENDING*' -print -quit)" ]]
for record in "$work/repeat-all"/**/*.run.txt "$work/recover"/**/*.run.txt "$work/collision"/**/*.run.txt; do
    [[ "$record" == *_start_*_KST_end_*_KST.run.txt ]]
    grep -q '^START_TIME_KST=' "$record"
    grep -q '^END_TIME_KST=' "$record"
    grep -q '^FINAL_EXIT_CODE=' "$record"
    if grep '^PRODUCER_CSV=' "$record" | grep -q '_end_PENDING'; then exit 1; fi
done
if grep -vE '^\[[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} KST\] ' "$work/initial.stdout" | grep -q .; then exit 1; fi
# 사용자가 실행 중 TERM을 보내도 자식 종료 후 회차 파일명을 확정한다.
PRODUCER_TEST_SECONDS=30 "$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --no-build --no-fio --no-iostat --output-dir "$work/interrupted" >"$work/interrupted.stdout" 2>&1 &
interrupted_pid=$!
for ((attempt=0; attempt<100; attempt++)); do
    if [[ -n "$(find "$work/interrupted" -name '*_producer.log' -print -quit 2>/dev/null)" ]]; then break; fi
    command sleep 0.1
done
kill -TERM "$interrupted_pid"
status=0
wait "$interrupted_pid" || status=$?
[[ "$status" -ne 0 ]]
[[ -z "$(find "$work/interrupted" -name '*_end_PENDING*' -print -quit)" ]]
grep -q '^END_TIME_KST=' "$work/interrupted"/**/*.run.txt
# 분산 모드: 서버 자원만 remote helper로, 토픽 API/생산/cleanup은 기존 클라이언트로 보낸다.
python3() {
    if [[ "$1" != */remote_broker.py ]]; then command python3 "$@"; return; fi
    shift
    while [[ "$1" == --* ]]; do shift 2; done
    echo "remote_$1" >> "$TRACE"
}
export -f python3
printf '' > "$TRACE"
"$real_bash" "$root/run_kafka_experiments.sh" --broker-ssh test@broker --broker-project-dir /broker \
    --hz 1 --topic-mode precreate --fio --drop-caches --no-iostat --no-delete --output-dir "$work/remote-routing" >/dev/null
[[ "$(tr '\n' ' ' < "$TRACE")" == 'remote_check remote_build remote_cache cleanup precreate remote_fio producer cleanup remote_cache ' ]]
grep -q '^BROKER_SSH=test@broker' "$work/remote-routing"/**/*.run.txt
# 원격 TRIM은 브로커 helper로 전달하고 대기가 끝난 뒤 클라이언트에서 사전 생성한다.
sleep() { SECONDS=$((SECONDS + 600)); command sleep 0.02; }
export -f sleep
printf '' > "$TRACE"
"$real_bash" "$root/run_kafka_experiments.sh" --broker-ssh test@broker --broker-project-dir /broker \
    --hz 1 --no-build --topic-mode precreate --fstrim --fio --no-iostat --no-delete --output-dir "$work/remote-trim" >/dev/null
[[ "$(tr '\n' ' ' < "$TRACE")" == 'remote_check cleanup remote_trim precreate remote_fio producer cleanup ' ]]
unset -f python3
# TRIM 경로: 실제 대기 대신 Bash 시간만 진행시켜 600초 하한/순서/반복/실패를 검증한다.
sleep() { SECONDS=$((SECONDS + 600)); command sleep 0.02; }
export -f sleep
printf '' > "$TRACE"
(unset FSTRIM_ENABLED; "$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --retry 2 --no-build --topic-mode precreate --no-fio --no-iostat --output-dir "$work/trim-default" >/dev/null)
[[ "$(tr '\n' ' ' < "$TRACE")" == 'cleanup trim precreate producer cleanup cleanup trim precreate producer cleanup ' ]]
grep -q '^FSTRIM_WAIT_SECONDS=600' "$work/trim-default"/**/*.run.txt
[[ "$(grep -h 'stage=FSTRIM_WAIT_END' "$work/trim-default"/**/*.run.txt | wc -l)" == 2 ]]
printf '' > "$TRACE"
"$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --no-build --fstrim --fstrim-wait-seconds 1200 --fio --no-iostat --output-dir "$work/trim-custom" >/dev/null
[[ "$(tr '\n' ' ' < "$TRACE")" == 'trim fio producer cleanup ' ]]
grep -q '^FSTRIM_WAIT_SECONDS=1200' "$work/trim-custom"/**/*.run.txt
printf '' > "$TRACE"
if FAIL_TRIM=true "$real_bash" "$root/run_kafka_experiments.sh" --hz 1 --retry 2 --no-build --fstrim --no-fio --no-iostat --output-dir "$work/trim-failed" >/dev/null 2>&1; then exit 1; fi
[[ "$(tr '\n' ' ' < "$TRACE")" == 'trim cleanup trim cleanup ' ]]
for invalid in 0 599 -1 abc; do
    if "$real_bash" "$root/run_kafka_experiments.sh" --fstrim-wait-seconds "$invalid" >/dev/null 2>&1; then exit 1; fi
done
unset -f sleep
echo "PASS: 회차 폴더/시작·종료 파일명/KST/실패·중단, 빌드·cleanup·캐시·분산 실행 경로. 테스트 로그: $work"
