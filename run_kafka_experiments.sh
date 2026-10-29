#!/usr/bin/env bash
# 생산·fio 준비·실시간 비활성 삭제·일회성 정리를 제어하는 Linux 통합 실행기.
set -Eeuo pipefail
# 옵션 해석 전 원본 인수와 호출 위치를 보존한다.
ORIGINAL_ARGS=("$@")
ORIGINAL_CWD="$PWD"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# 모든 터미널 출력(자식 도구 포함)에 출력 시점의 KST 시각을 붙인다.
export PYTHONIOENCODING=utf-8
PYTHON="$(command -v python3 || command -v python)" || { echo 'Python 3가 필요합니다.' >&2; exit 1; }
[[ -f "$SCRIPT_DIR/tools/kst_console.py" && -f "$SCRIPT_DIR/tools/finalize_experiment.py" ]] || { echo 'tools의 로그/파일명 관리 스크립트가 필요합니다.' >&2; exit 1; }
console_owner=$BASHPID
exec 3>&1 4>&2
exec > >("$PYTHON" -u "$SCRIPT_DIR/tools/kst_console.py" >&3 2>&4) 2>&1
console_pid=$!
finish_console() {
    if [[ "$BASHPID" == "$console_owner" ]]; then
        exec 1>&3 2>&4
        wait "$console_pid" || true
    fi
}
trap finish_console EXIT
fail() {
    echo "ERROR: $*" >&2
    if [[ -n "${record_file:-}" ]]; then
        record_event "failure_reason=$*"
        printf 'ERROR: %s\n' "$*" >> "${record_file%.run.txt}_failure.log"
    fi
    exit 1
}
usage() {
    # CLI 옵션은 환경 변수보다 우선한다. 각 옵션은 --이름 값 또는 --이름=값 형식이다.
    printf '%s\n' \
      "사용법: $0 [--all | --hz 3 | --mode fio|delete|cleanup] [옵션]" \
      "기본: --all (1 2 3 4 5 Hz)" \
      "분산 실행: --broker-ssh USER@HOST --broker-project-dir /data4/kafka-test-clients" \
      "원격: --remote-identity-file KEY --remote-stage-timeout-seconds 7200" \
      "빌드: --build (기본 ON) / --no-build (기존 apps JAR 재사용). 호출당 1회, 단독 모드 포함." \
      "반복: --retry N (각 Hz의 총 실행 횟수, 기본 1, 실패 재시도가 아닌 실험 반복)" \
      "토픽: --topic-mode auto|precreate|transient|transient-precreate (기본 auto)" \
      "토픽 준비 제한: --topic-prepare-timeout-seconds 300" \
      "precreate: 토픽 생성 → 선택적 fio → 데이터 생산 → cleanup (--no-delete --cleanup 필수)" \
      "transient: AutoTransientTopicProducerMetrics 사용, 일반 토픽 검사·사전 생성·JMX 삭제 생략" \
      "transient-precreate: 선택적 fio → Transient 전체 생성 → 전체 생산 완료 → 전체 seal → 선택적 reset" \
      "Transient 정리: --cleanup (기본 ON)으로 apps ResetTransientTopics 실행, --no-cleanup으로 생략" \
      "reset 재정의: --transient-reset-script PATH (선택, 로컬 외부 reset 스크립트)" \
      "reset 인수: --bootstrap-server BROKERS --timeout-ms MS; 모든 Transient 클라이언트 종료 후 클러스터 전체 초기화" \
      "스위치: --fio / --no-fio, --delete / --no-delete, --cleanup / --no-cleanup" \
      "기본 스위치: fio=on delete=off cleanup=on iostat=on" \
      "fio: --fio-target-dir --fio-size-gib --fio-passes --reserve-gib --expected-disk --fio-script" \
      "TRIM: --fstrim / --no-fstrim (실험 기본 ON), --fstrim-target-dir /data4" \
      "TRIM 대기: --fstrim-wait-seconds 600 (기본/최소 600초, 완료 후 대기; --expected-disk로 장치 확인)" \
      "생산: --brokers --prefix --topic-cnt --run-duration-minutes --topic-lifetime-ms" \
      "      --produce-hz-per-topic --msg-size --start-index --start-barrier-delay-ms" \
      "감시: --jmx-host --jmx-port --scan-interval-ms --scan-log-interval-ms" \
      "      --inactivity-check-interval-ms --inactivity-threshold-ms --deletion-mode" \
      "      --delete-start-timeout-seconds --delete-topic-prefix" \
      "정리: --cleanup-topic-prefix --cleanup-timeout-seconds" \
      "공통: --output-dir --inter-experiment-delay-seconds --kafka-home --daemon-source --client-dir --runner" \
      "단계 간격: --step-delay-seconds 10 (기본 10초, 0이면 비활성; 생략한 단계는 대기하지 않음)" \
      "캐시: --drop-caches / --no-drop-caches (실험 기본 ON, 실험 전후 sync 후 Linux drop_caches=3; 단독 모드 제외)" \
      "생산 플래그: --ignore-response --need-flush --share-producer --stats-disabled" \
      "시간 기반 delete: --delete-topic-prefix 기본값은 PREFIX_; 종료는 Ctrl+C" \
      "단독 cleanup: --cleanup-topic-prefix 필수" \
      "--iostat / --no-iostat: 디스크 통계를 1초마다 기록 (기본 켜짐)" \
      "cleanup은 단일/마지막 실험에도 적용하며 딜레이 시간에 정리 시간을 포함합니다." \
      "실행 추적: CSV와 동일 태그의 .run.txt 및 실행별 _runs.tsv를 자동 생성합니다."
    printf '%s\n' '결과: --output-layout flat(기본)|nested, --experiment-label LABEL (최대 24자)'
}
mode=experiment
rates=(1 2 3 4 5)
selection=""
extra_args=()
select_mode() {
    [[ -z "$selection" ]] || fail "--all, --hz, --mode는 한 번만 지정하세요."
    selection="$1"
}
# 이름이 확인된 변수에만 대입하며 eval은 사용하지 않는다.
while [[ $# -gt 0 ]]; do
    option="${1%%=*}"
    case "$option" in
        -h|--help) usage; exit 0 ;;
        --all|all) [[ "$1" != *=* ]] || fail "$option 에는 값이 없습니다."
            select_mode all; shift; continue ;;
        --build|--no-build|--fio|--no-fio|--delete|--no-delete|--cleanup|--no-cleanup|--stats-disabled|--stats-enabled|--iostat|--no-iostat|--drop-caches|--no-drop-caches)
            [[ "$1" != *=* ]] || fail "$option 은 값을 받지 않는 플래그입니다."
            case "$option" in
                --fio) FIO_ENABLED=true ;; --no-fio) FIO_ENABLED=false ;;
                --delete) DELETE_ENABLED=true ;; --no-delete) DELETE_ENABLED=false ;;
                --cleanup) CLEANUP_ENABLED=true ;; --no-cleanup) CLEANUP_ENABLED=false ;;
                --stats-disabled) STATS_DISABLED=true ;; --stats-enabled) STATS_DISABLED=false ;;
                --iostat) IOSTAT_ENABLED=true ;; --no-iostat) IOSTAT_ENABLED=false ;;
                --drop-caches) DROP_CACHES=true ;; --no-drop-caches) DROP_CACHES=false ;;
                --build) BUILD_ENABLED=true ;; --no-build) BUILD_ENABLED=false ;;
            esac
            shift; continue ;;
        --fstrim|--no-fstrim)
            [[ "$1" != *=* ]] || fail "$option 은 값을 받지 않는 플래그입니다."
            if [[ "$option" == --fstrim ]]; then FSTRIM_ENABLED=true; else FSTRIM_ENABLED=false; fi
            shift; continue ;;
        --fstrim-target-dir|--fstrim-wait-seconds|--broker-ssh|--broker-project-dir|--remote-identity-file|--remote-stage-timeout-seconds) ;;
        --ignore-response|--need-flush|--share-producer)
            [[ "$1" != *=* ]] || fail "$option 은 값을 받지 않는 플래그입니다."
            extra_args+=("$1"); shift; continue ;;
        --) shift; extra_args+=("$@"); break ;;
        fio|delete|cleanup) select_mode "$option"; mode="$option"; shift; continue ;;
        --output-layout|--experiment-label|--step-delay-seconds|--topic-mode|--topic-prepare-timeout-seconds|--transient-reset-script|--retry|--hz|--mode|--brokers|--prefix|--topic-cnt|--run-duration-minutes|--topic-lifetime-ms|--produce-hz-per-topic|--msg-size|--start-index|--start-barrier-delay-ms|--fio-target-dir|--fio-size-gib|--fio-passes|--reserve-gib|--expected-disk|--fio-script|--jmx-host|--jmx-port|--scan-interval-ms|--scan-log-interval-ms|--inactivity-check-interval-ms|--inactivity-threshold-ms|--deletion-mode|--delete-start-timeout-seconds|--delete-topic-prefix|--cleanup-topic-prefix|--cleanup-timeout-seconds|--output-dir|--inter-experiment-delay-seconds|--kafka-home|--daemon-source|--client-dir|--runner) ;;
        *) if [[ "$1" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
               select_mode hz; rates=("$1"); shift; continue
           fi
           fail "알 수 없는 옵션: $1 (--help 참고)" ;;
    esac
    if [[ "$1" == *=* ]]; then
        value="${1#*=}"; shift
    else
        [[ $# -ge 2 && "$2" != --* ]] || fail "$option 값이 필요합니다."
        value="$2"; shift 2
    fi
    [[ -n "$value" ]] || fail "$option 값이 비어 있습니다."
    case "$option" in
        --hz) select_mode hz; rates=("$value") ;;
        --mode) select_mode mode
            case "$value" in experiment|fio|delete|cleanup) mode="$value" ;; *) fail "잘못된 모드: $value" ;; esac ;;
        *) variable="${option#--}"; variable="${variable//-/_}"
            printf -v "${variable^^}" '%s' "$value" ;;
    esac
done
# 추가 생산기 인수만 이후 실행 단계에 전달한다.
OUTPUT_LAYOUT="${OUTPUT_LAYOUT:-flat}"
EXPERIMENT_LABEL="${EXPERIMENT_LABEL:-run}"
[[ "$OUTPUT_LAYOUT" == flat || "$OUTPUT_LAYOUT" == nested ]] || fail '--output-layout은 flat 또는 nested입니다.'
[[ "$EXPERIMENT_LABEL" =~ ^[a-zA-Z0-9_-]{1,24}$ ]] || fail '--experiment-label은 영숫자/_/- 1~24자입니다.'
set -- "${extra_args[@]}"
[[ "$mode" == experiment || $# -eq 0 ]] || fail "단독 모드에서는 생산기 옵션을 사용할 수 없습니다."
# fio 자식 스크립트에서 읽는 설정을 명시적으로 전달한다.
export FIO_PASSES="${FIO_PASSES:-3}" RESERVE_GIB="${RESERVE_GIB:-2}" EXPECTED_DISK="${EXPECTED_DISK:-nvme1n1}"
# 이전 배포 위치와 압축 해제된 로컬 프로젝트를 모두 지원한다.
CLIENT_DIR="${CLIENT_DIR:-$SCRIPT_DIR}"
if [[ ! -f "$CLIENT_DIR/scripts/kafka_clients_run.sh" ]]; then
    if [[ -f "$SCRIPT_DIR/kafka_clients_run.sh" ]]; then CLIENT_DIR="$(dirname "$SCRIPT_DIR")"
    elif [[ -f "$SCRIPT_DIR/kafka-test-clients_extracted/kafka-test-clients/scripts/kafka_clients_run.sh" ]]; then
        CLIENT_DIR="$SCRIPT_DIR/kafka-test-clients_extracted/kafka-test-clients"
    fi
fi
RUNNER="${RUNNER:-$CLIENT_DIR/scripts/kafka_clients_run.sh}"
BROKERS="${BROKERS:-166.104.110.11:9092}"
PREFIX="${PREFIX:-test_creator}"
# 삭제 판정은 비활성 시간으로 하며 접두어는 관리 대상 범위만 제한한다.
DELETE_TOPIC_PREFIX="${DELETE_TOPIC_PREFIX:-${PREFIX}_}"
[[ -n "$DELETE_TOPIC_PREFIX" && "$DELETE_TOPIC_PREFIX" != __* ]] || fail "유효한 --delete-topic-prefix가 필요합니다."
OUTPUT_DIR="${OUTPUT_DIR:-$SCRIPT_DIR/renewed_output/auto_topic_producer_metrics}"
FIO_ENABLED="${FIO_ENABLED:-true}"
DELETE_ENABLED="${DELETE_ENABLED:-false}"
CLEANUP_ENABLED="${CLEANUP_ENABLED:-true}"
IOSTAT_ENABLED="${IOSTAT_ENABLED:-true}"
FIO_SCRIPT="${FIO_SCRIPT:-$SCRIPT_DIR/prepare_ssd_fio.sh}"
FIO_TARGET_DIR="${FIO_TARGET_DIR:-/data4}"
FIO_SIZE_GIB="${FIO_SIZE_GIB:-auto}"
INTER_EXPERIMENT_DELAY_SECONDS="${INTER_EXPERIMENT_DELAY_SECONDS:-60}"
STEP_DELAY_SECONDS="${STEP_DELAY_SECONDS:-10}"
FSTRIM_TARGET_DIR="${FSTRIM_TARGET_DIR:-$FIO_TARGET_DIR}"
FSTRIM_WAIT_SECONDS="${FSTRIM_WAIT_SECONDS:-600}"
[[ "$FSTRIM_WAIT_SECONDS" =~ ^[0-9]{1,6}$ ]] && (( 10#$FSTRIM_WAIT_SECONDS >= 600 )) || fail "--fstrim-wait-seconds는 최소 600초(10분)의 정수입니다."
# 모든 실험의 기본값은 ON. 전후 실험 단계가 없는 단독 도구는 기존 OFF를 유지한다.
if [[ "$mode" == experiment ]]; then
    DROP_CACHES="${DROP_CACHES:-true}"
    FSTRIM_ENABLED="${FSTRIM_ENABLED:-true}"
else
    DROP_CACHES="${DROP_CACHES:-false}"
    FSTRIM_ENABLED="${FSTRIM_ENABLED:-false}"
fi
[[ "$FSTRIM_ENABLED" == true || "$FSTRIM_ENABLED" == false ]] || fail "FSTRIM_ENABLED는 true/false입니다."
[[ "$FSTRIM_ENABLED" == false || "$mode" == experiment ]] || fail "--fstrim은 실험 모드에서만 지원합니다."
BUILD_ENABLED="${BUILD_ENABLED:-true}"
RETRY="${RETRY:-1}"
# 반복 횟수는 총 실행 횟수이며, 단독 도구 모드에는 반복을 적용하지 않는다.
[[ "$RETRY" =~ ^[1-9][0-9]{0,3}$ ]] || fail "--retry는 1~9999의 정수여야 합니다."
[[ "$mode" == experiment || "$RETRY" == 1 ]] || fail "--retry 반복은 실험 모드(--all/--hz)에서만 지원합니다."
retry_index=1
TOPIC_CNT="${TOPIC_CNT:--1}"
TOPIC_MODE="${TOPIC_MODE:-auto}"
TOPIC_PREPARE_TIMEOUT_SECONDS="${TOPIC_PREPARE_TIMEOUT_SECONDS:-300}"
TRANSIENT_RESET_SCRIPT="${TRANSIENT_RESET_SCRIPT:-}"
PRODUCER_CLASS=AutoTopicProducerMetrics
case "$TOPIC_MODE" in
    auto|precreate) TRANSIENT_MODE=false ;;
    transient|transient-precreate) TRANSIENT_MODE=true ;;
    *) fail "--topic-mode는 auto, precreate, transient 또는 transient-precreate입니다." ;;
esac
[[ "$mode" != experiment || "$TRANSIENT_MODE" == true || "$CLEANUP_ENABLED" == true ]] || fail "동일 토픽 이름을 재사용하므로 일반 토픽 실험에서는 --cleanup이 필수입니다."
[[ "$TOPIC_PREPARE_TIMEOUT_SECONDS" =~ ^[1-9][0-9]{0,4}$ ]] || fail "토픽 준비 제한 시간은 1~99999초입니다."
if [[ "$TOPIC_MODE" == precreate ]]; then
    [[ "$mode" == experiment && "$DELETE_ENABLED" == false && "$CLEANUP_ENABLED" == true ]] || fail "precreate는 실험 모드에서 --no-delete --cleanup으로 실행하세요. 생산 전 빈 토픽 삭제를 방지합니다."
fi
RUN_DURATION_MINUTES="${RUN_DURATION_MINUTES:-60}"
TOPIC_LIFETIME_MS="${TOPIC_LIFETIME_MS:-40000}"
PRODUCE_HZ_PER_TOPIC="${PRODUCE_HZ_PER_TOPIC:-100}"
MSG_SIZE="${MSG_SIZE:-4096}"
START_INDEX="${START_INDEX:-0}"
START_BARRIER_DELAY_MS="${START_BARRIER_DELAY_MS:-10000}"
STATS_DISABLED="${STATS_DISABLED:-false}"
KAFKA_HOME="${KAFKA_HOME:-/data4/kafka-tt}"
DAEMON_SOURCE="${DAEMON_SOURCE:-$SCRIPT_DIR/KafkaJmxProducerBatchDaemon.java}"
JMX_HOST="${JMX_HOST:-127.0.0.1}"
JMX_PORT="${JMX_PORT:-9999}"
SCAN_INTERVAL_MS="${SCAN_INTERVAL_MS:-1000}"
SCAN_LOG_INTERVAL_MS="${SCAN_LOG_INTERVAL_MS:-1000}"
INACTIVITY_CHECK_INTERVAL_MS="${INACTIVITY_CHECK_INTERVAL_MS:-1000}"
INACTIVITY_THRESHOLD_MS="${INACTIVITY_THRESHOLD_MS:-600000}"
DELETION_MODE="${DELETION_MODE:-BATCH}"
CLEANUP_TIMEOUT_SECONDS="${CLEANUP_TIMEOUT_SECONDS:-300}"
DELETE_START_TIMEOUT_SECONDS="${DELETE_START_TIMEOUT_SECONDS:-60}"
for value in "$FIO_ENABLED" "$DELETE_ENABLED" "$CLEANUP_ENABLED" "$STATS_DISABLED" "$IOSTAT_ENABLED" "$DROP_CACHES" "$BUILD_ENABLED"; do
    [[ "$value" == true || "$value" == false ]] || fail "기능 스위치는 true/false로 지정하세요."
done
if [[ "$TRANSIENT_MODE" == true ]]; then
    [[ "$mode" == experiment ]] || fail "--topic-mode $TOPIC_MODE 모드는 실험 모드(--all/--hz)에서만 지원합니다."
    [[ "$DELETE_ENABLED" == false ]] || fail "$TOPIC_MODE 모드에서는 일반 토픽 JMX 삭제기(--delete)를 사용할 수 없습니다."
    for arg in "$@"; do
        case "$arg" in
            --ignore-response|--ignore-response=*) fail "$TOPIC_MODE 모드는 acks=all이 필요하므로 --ignore-response를 사용할 수 없습니다." ;;
            --check-topics-absent*|--prepare-topics-only*|--require-existing-topics*) fail "$TOPIC_MODE 모드에서는 일반 토픽 준비 옵션을 사용할 수 없습니다: $arg" ;;
        esac
        if [[ "$TOPIC_MODE" == transient-precreate ]]; then
            case "$arg" in
                --precreate|--precreate=*|--topic-prepare-timeout-seconds*)
                    fail "transient-precreate의 사전 생성/준비 제한은 실행기가 관리합니다: $arg (--topic-prepare-timeout-seconds를 -- 앞에 지정하세요)" ;;
            esac
        fi
    done
    if [[ "$CLEANUP_ENABLED" == true ]]; then
        if [[ -n "$TRANSIENT_RESET_SCRIPT" ]]; then
            [[ -f "$TRANSIENT_RESET_SCRIPT" && -r "$TRANSIENT_RESET_SCRIPT" ]] || fail "읽을 수 있는 Transient reset 스크립트가 필요합니다: $TRANSIENT_RESET_SCRIPT"
        fi
        [[ "$CLEANUP_TIMEOUT_SECONDS" =~ ^[1-9][0-9]{0,4}$ ]] && (( CLEANUP_TIMEOUT_SECONDS <= 86400 )) || fail "Transient reset 제한 시간은 1~86400초입니다."
    fi
    PRODUCER_CLASS=AutoTransientTopicProducerMetrics
fi
CLEANUP_POLICY=none
if [[ "$CLEANUP_ENABLED" == true ]]; then
    if [[ "$TRANSIENT_MODE" == true ]]; then CLEANUP_POLICY=reset-transient-topics
    else CLEANUP_POLICY=delete-topics; fi
fi
[[ "$INTER_EXPERIMENT_DELAY_SECONDS" =~ ^[0-9]{1,6}$ ]] || fail "딜레이는 0 이상의 정수입니다."
[[ "$STEP_DELAY_SECONDS" =~ ^[0-9]{1,6}$ ]] || fail "--step-delay-seconds는 0 이상의 정수입니다."
# 분산 모드에서 서버 자원 작업만 SSH로 보낸다.
BROKER_SSH="${BROKER_SSH:-}"
BROKER_PROJECT_DIR="${BROKER_PROJECT_DIR:-/data4/kafka-test-clients}"
REMOTE_IDENTITY_FILE="${REMOTE_IDENTITY_FILE:-}"
REMOTE_STAGE_TIMEOUT_SECONDS="${REMOTE_STAGE_TIMEOUT_SECONDS:-7200}"
remote_command=()
if [[ -n "$BROKER_SSH" ]]; then
    [[ "$REMOTE_STAGE_TIMEOUT_SECONDS" =~ ^[1-9][0-9]{0,5}$ ]] && (( REMOTE_STAGE_TIMEOUT_SECONDS <= 604800 )) || fail "원격 제한 시간은 1~604800초입니다."
    command -v ssh >/dev/null || fail "SSH가 필요합니다."
    [[ -f "$SCRIPT_DIR/tools/remote_broker.py" ]] || fail "tools/remote_broker.py가 필요합니다."
    remote_command=("$PYTHON" "$SCRIPT_DIR/tools/remote_broker.py" --host "$BROKER_SSH" --project "$BROKER_PROJECT_DIR" --identity "$REMOTE_IDENTITY_FILE" --timeout "$REMOTE_STAGE_TIMEOUT_SECONDS")
fi
cache_drop_command=(sh -c 'sync && printf "3\n" > /proc/sys/vm/drop_caches')
if [[ "$DROP_CACHES" == true ]]; then
    [[ "$mode" == experiment ]] || fail "--drop-caches는 실험 모드에서만 지원합니다."
    [[ "$(uname -s)" == Linux ]] || fail "--drop-caches는 Linux에서만 지원합니다."
    if [[ -z "$BROKER_SSH" ]]; then
      command -v sync >/dev/null || fail "sync 명령이 필요합니다."
      if (( EUID != 0 )); then
        command -v sudo >/dev/null || fail "캐시 비우기에 sudo가 필요합니다."
        sudo -n true || fail "캐시 비우기 권한이 없습니다. sudo -v 후 실행하거나 해당 명령의 sudo 권한을 설정하세요."
        cache_drop_command=(sudo -n "${cache_drop_command[@]}")
      fi
    fi
fi
[[ "$DELETE_START_TIMEOUT_SECONDS" =~ ^[1-9][0-9]{0,3}$ ]] || fail "감시 시작 제한 시간은 양의 정수입니다."
# 별도의 세션으로 실행하여 자식 Java/fio 프로세스까지 종료할 수 있게 한다.
command -v setsid >/dev/null || fail "Linux setsid 명령이 필요합니다."
if [[ "$IOSTAT_ENABLED" == true && -z "$BROKER_SSH" ]]; then
    command -v iostat >/dev/null || fail "iostat 명령이 필요합니다. sysstat 패키지를 설치하거나 --no-iostat을 지정하세요."
fi
mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"
RUN_ID="$(TZ=KST-9 date +%Y%m%d_%H%M%S)_$$_${RANDOM}"
if [[ -n "$BROKER_SSH" ]]; then
    remote_checks=()
    [[ "$DROP_CACHES" != true ]] || remote_checks+=(cache-permission)
    [[ "$IOSTAT_ENABLED" != true ]] || remote_checks+=(iostat)
    [[ "$FIO_ENABLED" != true && "$mode" != fio ]] || remote_checks+=(fio)
    [[ "$FSTRIM_ENABLED" != true ]] || remote_checks+=(fstrim findmnt lsblk)
    "${remote_command[@]}" --run "$RUN_ID" check "${remote_checks[@]}" || fail "원격 서버 준비 실패: SSH 인증·host key·도구·sudo 권한을 확인하세요."
    cache_drop_command=("${remote_command[@]}" --run "$RUN_ID" cache)
fi
record_file=""
run_index="$OUTPUT_DIR/${RUN_ID}_runs.tsv"
build_log="$OUTPUT_DIR/${RUN_ID}_build.log"
built_jar="$CLIENT_DIR/build/libs/apps-1.0-SNAPSHOT-all.jar"
build_metadata="$OUTPUT_DIR/${RUN_ID}_build.txt"
record_command() {
    [[ -n "$record_file" ]] || return 0
    local label="$1"
    shift
    # %q를 사용하여 공백을 포함한 인수의 경계까지 보존한다.
    { printf '%s=' "$label"; printf '%q ' "$@"; printf '\n'; } >> "$record_file"
}
record_event() {
    [[ -n "$record_file" ]] || return 0
    printf 'EVENT utc=%s kst=%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(TZ=KST-9 date +%Y-%m-%dT%H:%M:%S+09:00)" "$*" >> "$record_file"
}
# 화면에 설명한 적용 설정을 동일하게 실행 기록에도 남긴다.
describe_step() {
    printf '%s\n' "$*"
    record_event "description=$*"
}
# 실제 실행하는 단계 사이에서만 1초마다 대기 진행을 기록한다.
begin_step() {
    local next="$1" started=$SECONDS delay=$((10#$STEP_DELAY_SECONDS))
    if [[ -n "${previous_step:-}" ]]; then
        record_event "stage=STEP_DELAY from=$previous_step to=$next requested_seconds=$STEP_DELAY_SECONDS"
        while (( SECONDS - started < delay )); do
            [[ -z "$iostat_pid" ]] || kill -0 "$iostat_pid" 2>/dev/null || fail "단계 대기 중 iostat 종료"
            [[ -z "$delete_pid" ]] || kill -0 "$delete_pid" 2>/dev/null || fail "단계 대기 중 삭제기 종료"
            describe_step "[단계 간 대기] $previous_step → $next: 경과=$((SECONDS - started))초, 남은 시간=$((delay - SECONDS + started))초"
            sleep 1
        done
    fi
    previous_step="$next"
}
run_fstrim() {
    local log="$OUTPUT_DIR/${tag}_fstrim.log" started elapsed remaining
    local -a trim_command=(bash "$SCRIPT_DIR/prepare_ssd_trim.sh")
    if [[ -n "$BROKER_SSH" ]]; then trim_command=("${remote_command[@]}" --run "${RUN_ID}_trim" trim); fi
    describe_step "STEP 0-T: fstrim ON — 실행 서버=${BROKER_SSH:-$(hostname)}, 경로=$FSTRIM_TARGET_DIR, 장치=$EXPECTED_DISK, 완료 후 대기=${FSTRIM_WAIT_SECONDS}초"
    record_event 'stage=FSTRIM_START'
    record_command FSTRIM_COMMAND setsid "${trim_command[@]}" "$FSTRIM_TARGET_DIR" "$EXPECTED_DISK"
    setsid "${trim_command[@]}" "$FSTRIM_TARGET_DIR" "$EXPECTED_DISK" >"$log" 2>&1 &
    job_pid=$!
    wait_job 'STEP 0-T fstrim' "$log"
    record_event 'stage=FSTRIM_SUCCESS'
    started=$SECONDS
    record_event "stage=FSTRIM_WAIT_START requested_seconds=$FSTRIM_WAIT_SECONDS"
    while (( SECONDS - started < 10#$FSTRIM_WAIT_SECONDS )); do
        [[ -z "$iostat_pid" ]] || kill -0 "$iostat_pid" 2>/dev/null || fail "TRIM 대기 중 iostat 종료"
        elapsed=$((SECONDS - started))
        remaining=$((10#$FSTRIM_WAIT_SECONDS - elapsed))
        describe_step "[STEP 0-T TRIM 대기] 경과=${elapsed}초, 남은 시간=${remaining}초"
        sleep 1
    done
    record_event "stage=FSTRIM_WAIT_END elapsed_seconds=$((SECONDS - started))"
}
drop_caches() {
    local phase="$1" log="$OUTPUT_DIR/${tag}_drop_caches_${1}.log"
    [[ "$DROP_CACHES" == true ]] || return 0
    describe_step "[캐시 비우기 $phase] 실행 서버=${BROKER_SSH:-$(hostname)}, sync 후 drop_caches=3 (페이지 캐시·dentry·inode)"
    if [[ -n "$BROKER_SSH" ]]; then cache_drop_command=("${remote_command[@]}" --run "${RUN_ID}_cache_${phase}" cache); fi
    record_event "stage=DROP_CACHES_START phase=$phase"
    record_command "DROP_CACHES_${phase^^}_COMMAND" setsid "${cache_drop_command[@]}"
    setsid "${cache_drop_command[@]}" >"$log" 2>&1 &
    job_pid=$!
    wait_job "캐시 비우기 $phase" "$log"
    record_event "stage=DROP_CACHES_SUCCESS phase=$phase"
}
start_record() {
    local tag="$1" rate="$2" setting source_file
    record_file="$OUTPUT_DIR/${tag}.run.txt"
    # 본문 CSV의 스키마는 유지하고 동일 태그의 메타데이터 파일로 연결한다.
    {
        printf 'FORMAT_VERSION=4\nRUN_ID=%s\nTAG=%s\nMODE=%s\nHZ=%s\n' "$RUN_ID" "$tag" "$mode" "$rate"
        printf 'START_TIME_KST=%s\n' "${round_start_iso:-$(TZ=KST-9 date +%Y-%m-%dT%H:%M:%S+09:00)}"
        printf 'RETRY=%s\nRETRY_INDEX=%s\n' "$RETRY" "$retry_index"
        printf 'OUTPUT_LAYOUT=%s\nEXPERIMENT_LABEL=%s\n' "$OUTPUT_LAYOUT" "$EXPERIMENT_LABEL"
        printf 'TOPIC_MODE=%s\nTOPIC_PREPARE_TIMEOUT_SECONDS=%s\n' "$TOPIC_MODE" "$TOPIC_PREPARE_TIMEOUT_SECONDS"
        printf 'PRODUCER_CLASS=%s\nCLEANUP_POLICY=%s\nTRANSIENT_RESET_SCRIPT=%q\n' "$PRODUCER_CLASS" "$CLEANUP_POLICY" "$TRANSIENT_RESET_SCRIPT"
        if [[ "$TOPIC_MODE" == precreate ]]; then
            printf 'PRECREATE_SEED_MESSAGES_PER_TOPIC=1\nPRECREATE_SEED_ACKS=all\nPRECREATE_SEED_INCLUDED_IN_METRICS=false\n'
        else
            printf 'PRECREATE_SEED_MESSAGES_PER_TOPIC=0\n'
        fi
        printf 'STEP_DELAY_SECONDS=%s\nDROP_CACHES=%s\n' "$STEP_DELAY_SECONDS" "$DROP_CACHES"
        printf 'BUILD_ENABLED=%s\n' "$BUILD_ENABLED"
        printf 'FSTRIM_ENABLED=%s\nFSTRIM_TARGET_DIR=%q\nFSTRIM_WAIT_SECONDS=%s\n' "$FSTRIM_ENABLED" "$FSTRIM_TARGET_DIR" "$FSTRIM_WAIT_SECONDS"
        printf 'BROKER_SSH=%q\nBROKER_PROJECT_DIR=%q\nREMOTE_STAGE_TIMEOUT_SECONDS=%s\n' "$BROKER_SSH" "$BROKER_PROJECT_DIR" "$REMOTE_STAGE_TIMEOUT_SECONDS"
        printf 'CLIENT_EXECUTION_HOST=%q\nRESOURCE_EXECUTION_HOST=%q\n' "$(hostname)" "${BROKER_SSH:-$(hostname)}"
        printf 'BUILD_LOG=%q\nBUILD_METADATA=%q\nBUILT_JAR=%q\n' "$build_log" "$build_metadata" "$built_jar"
        printf 'IOSTAT_ENABLED=%s\nIOSTAT_INTERVAL_SECONDS=1\nIOSTAT_LOG=%q\n' "$IOSTAT_ENABLED" "$OUTPUT_DIR/${tag}_iostat.log"
        printf 'ORIGINAL_CWD=%q\nHOST=%q\n' "$ORIGINAL_CWD" "${HOSTNAME:-unknown}"
        printf 'PRODUCER_CSV=%q\nDELETION_CSV=%q\n' "$OUTPUT_DIR/${tag}_metrics.csv" "$OUTPUT_DIR/${tag}_deletion.csv"
        printf 'CSV_NOTE=Paths are expected outputs; disabled or failed stages may not create them.\n'
        for setting in BROKERS PREFIX DELETE_TOPIC_PREFIX FIO_ENABLED DELETE_ENABLED CLEANUP_ENABLED STATS_DISABLED TOPIC_CNT RUN_DURATION_MINUTES TOPIC_LIFETIME_MS PRODUCE_HZ_PER_TOPIC MSG_SIZE START_INDEX START_BARRIER_DELAY_MS JMX_HOST JMX_PORT SCAN_INTERVAL_MS SCAN_LOG_INTERVAL_MS INACTIVITY_CHECK_INTERVAL_MS INACTIVITY_THRESHOLD_MS DELETION_MODE DELETE_START_TIMEOUT_SECONDS CLEANUP_TIMEOUT_SECONDS INTER_EXPERIMENT_DELAY_SECONDS FIO_TARGET_DIR FIO_SIZE_GIB FIO_PASSES RESERVE_GIB EXPECTED_DISK OUTPUT_DIR CLIENT_DIR RUNNER FIO_SCRIPT DAEMON_SOURCE KAFKA_HOME; do
            printf '%s=%q\n' "$setting" "${!setting}"
        done
        printf 'EXPERIMENT_PREFIX=%q\n' "${experiment_prefix:-}"
        # 기록 대상은 실행 파일로 한정하고 전체 환경 변수는 저장하지 않는다.
        if command -v sha256sum >/dev/null; then
            for source_file in "$SCRIPT_DIR/run_kafka_experiments.sh" "$FIO_SCRIPT" "$DAEMON_SOURCE" "$RUNNER" "$built_jar" "$TRANSIENT_RESET_SCRIPT"; do
                if [[ -f "$source_file" ]]; then sha256sum -- "$source_file"; fi
            done
        fi
    } > "$record_file"
    record_command ORIGINAL_COMMAND bash "$SCRIPT_DIR/run_kafka_experiments.sh" "${ORIGINAL_ARGS[@]}"
    if [[ ! -f "$run_index" ]]; then
        printf 'tag\thz\tmetadata\tproducer_csv\tdeletion_csv\tiostat_log\tretry_index\tretry_total\n' > "$run_index"
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$tag" "$rate" "$record_file" "$OUTPUT_DIR/${tag}_metrics.csv" "$OUTPUT_DIR/${tag}_deletion.csv" "$OUTPUT_DIR/${tag}_iostat.log" "$retry_index" "$RETRY" >> "$run_index"
    record_event 'status=STARTED'
    echo "실행 설정 기록: $record_file"
    [[ "$IOSTAT_ENABLED" == false ]] || start_iostat "$tag"
}
finish_record() {
    if [[ -n "$iostat_pid" ]]; then
        kill -0 "$iostat_pid" 2>/dev/null || fail "iostat이 비정상 종료되었습니다."
    fi
    stop_group "$iostat_pid"
    iostat_pid=""
    record_event 'status=SUCCESS exit_code=0'
    record_file=""
}
delete_pid=""
job_pid=""
iostat_pid=""
start_iostat() {
    local log="$OUTPUT_DIR/${1}_iostat.log"
    local -a disk_command=(iostat -d -x -t -y 1)
    if [[ -n "$BROKER_SSH" ]]; then disk_command=("${remote_command[@]}" --timeout 604800 --run "${RUN_ID}_iostat" iostat); fi
    record_event 'stage=IOSTAT_START'
    record_command IOSTAT_COMMAND setsid "${disk_command[@]}"
    # -y: 부팅 이후 누적값을 생략하고, 모든 디스크의 확장 통계를 1초 간격으로 수집한다.
    LC_ALL=C S_TIME_FORMAT=ISO setsid "${disk_command[@]}" >"$log" 2>&1 &
    iostat_pid=$!
    echo "iostat 1초 기록: $log"
}
stop_group() {
    local pid="$1" label="${2:-}" attempt
    [[ -n "$pid" ]] || return 0
    [[ -z "$label" ]] || describe_step "[$label] 종료 요청: PID=$pid, 정상 종료를 최대 10초 기다립니다."
    kill -TERM -- "-$pid" 2>/dev/null || true
    # 정상 종료에 최대 10초를 허용한 뒤 남은 자식도 정리한다.
    for ((attempt=0; attempt<10; attempt++)); do
        kill -0 -- "-$pid" 2>/dev/null || break
        [[ -z "$label" ]] || describe_step "[$label] 종료 대기 중: PID=$pid, 경과=${attempt}초/10초"
        sleep 1
    done
    if kill -0 -- "-$pid" 2>/dev/null; then
        [[ -z "$label" ]] || describe_step "[$label] 정상 종료 제한 초과: 남은 프로세스에 강제 종료를 요청합니다."
    fi
    kill -KILL -- "-$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    [[ -z "$label" ]] || describe_step "[$label] 프로세스 종료 대기 완료: PID=$pid"
}
recovery_exit() {
    local status=$?
    trap - EXIT ERR
    stop_group "$job_pid"
    exit "$status"
}
on_exit() {
    local status=$?
    trap - EXIT ERR
    # EXIT 트랩을 재등록해도 재호출되지 않으므로 모든 경로에서 명시적으로 마무리한다.
    stop_group "$job_pid"
    stop_group "$delete_pid"
    stop_group "$iostat_pid"
    job_pid=""; delete_pid=""; iostat_pid=""
    if [[ "${experiment_active:-false}" == true && "${interrupted:-false}" == false && "$status" -ne 0 ]]; then
        record_event "status=FAILED exit_code=$status"
        printf 'FAILED exit_code=%s; metadata=%s\n' "$status" "$record_file" >> "${record_file%.run.txt}_failure.log"
        if [[ "${experiment_cleanup_allowed:-false}" != true ]]; then
            record_event 'status=PREFLIGHT_FAILED cleanup_skipped=true'
            if [[ "${cache_before_done:-false}" == true ]]; then
                (drop_caches after_failure) || status=1
            fi
            echo '시작 전 준비/검사 실패: 추가 복구 삭제 없이 전체 실행을 중단합니다.' >&2
            complete_exit "$status"
        fi
        if [[ "$TRANSIENT_MODE" == true ]]; then
            if [[ "$CLEANUP_ENABLED" == false || "${transient_reset_attempted:-false}" == true ]]; then
                # reset 실패/시간 초과는 부분 초기화일 수 있으므로 자동으로 재시도하지 않는다.
                record_event "status=RECOVERY_SKIPPED cleanup_policy=$CLEANUP_POLICY reset_attempted=${transient_reset_attempted:-false} next_experiment_allowed=false"
                describe_step '[실험 실패] Transient reset을 자동 재시도하지 않고 전체 실행을 중단합니다. --no-cleanup이면 복구 reset도 생략합니다.'
                if [[ "${cache_before_done:-false}" == true ]]; then
                    (drop_caches after_failure) || status=1
                fi
                complete_exit "$status"
            fi
        fi
        describe_step "[실험 실패] 현재 실험 토픽을 정리합니다 (--no-cleanup이어도 실패 시 정리)."
        # 복구 중 오류는 재복구하지 않고 전체 실행을 중단한다.
        # EXIT 처리 중 EXIT 트랩을 교체해도 재호출되지 않으므로 별도 셸에서 복구한다.
        set +e
        (
        set -e
        trap recovery_exit EXIT
        trap 'stop_group "$job_pid"; exit 130' INT
        trap 'stop_group "$job_pid"; exit 143' TERM
        trap 'stop_group "$job_pid"; exit 129' HUP
        local recovery_started=$SECONDS recovery_remaining
        record_event 'stage=FAILURE_CLEANUP_START'
        run_cleanup "${experiment_prefix}_0_" "${tag}_recovery"
        record_event 'stage=FAILURE_CLEANUP_SUCCESS'
        drop_caches after_failure
        while (( SECONDS - recovery_started < 10#$INTER_EXPERIMENT_DELAY_SECONDS )); do
            recovery_remaining=$((10#$INTER_EXPERIMENT_DELAY_SECONDS - (SECONDS - recovery_started)))
            describe_step "[실패 후 대기] 경과=$((SECONDS - recovery_started))초, 남은 시간=${recovery_remaining}초"
            sleep 1
        done
        record_event 'status=FAILED_CLEANED next_experiment_allowed=true'
        exit 20
        )
        local recovery_status=$?
        # EXIT 처리에서 시작한 서브셸의 EXIT 트랩 동작은 Bash 환경에 따라 달라진다.
        # 복구 결과는 항상 부모가 수집하고 기록한다.
        if [[ "$recovery_status" -ne 20 ]]; then
            record_event "status=RECOVERY_FAILED exit_code=$recovery_status"
            printf 'RECOVERY_FAILED exit_code=%s\n' "$recovery_status" >> "${record_file%.run.txt}_failure.log"
        fi
        complete_exit "$recovery_status"
    fi
    record_event "status=EXIT exit_code=$status"
    complete_exit "$status"
}
complete_exit() {
    local result="$1"
    finalize_round "$result" || result=1
    finish_console
    exit "$result"
}
finalize_round() {
    [[ "${round_started:-false}" == true ]] || return 0
    round_started=false
    "$PYTHON" "$SCRIPT_DIR/tools/finalize_experiment.py" --directory "$OUTPUT_DIR" --tag "$tag" --index "$run_index" --status "$1"
}
trap on_exit EXIT
trap 'interrupted=true; exit 130' INT
trap 'interrupted=true; exit 143' TERM
trap 'interrupted=true; exit 129' HUP
# 감시기가 실행 중 죽으면 생산 작업도 즉시 중단한다.
wait_job() {
    local status=0 progress_label="${1:-}" progress_log="${2:-}" started=$SECONDS next_report=$SECONDS latest
    local duration_seconds="${3:-}" elapsed remaining
    local timeout_seconds="${4:-}"
    while kill -0 "$job_pid" 2>/dev/null; do
        if [[ -n "$timeout_seconds" ]] && (( SECONDS - started >= timeout_seconds )); then
            fail "작업 시간 초과: ${timeout_seconds}초, 상세 로그=$progress_log"
        fi
        if [[ -n "$iostat_pid" ]] && ! kill -0 "$iostat_pid" 2>/dev/null; then
            fail "iostat이 종료되었습니다. 디스크 상태 로그를 확인하세요."
        fi
        if [[ -n "$delete_pid" ]] && ! kill -0 "$delete_pid" 2>/dev/null; then
            fail "실시간 삭제기가 종료되었습니다. 감시 로그를 확인하세요."
        fi
        # Java의 최근 로그를 재사용하므로 상태 표시를 위한 Kafka/JMX 추가 조회는 없다.
        if [[ -n "$progress_label" ]] && (( SECONDS >= next_report )); then
            if [[ -n "$duration_seconds" ]]; then
                # 설정 시간 기준 카운트다운이다. 0초가 되어도 실제 생산 종료를 기다린다.
                elapsed=$((SECONDS - started))
                remaining=$((duration_seconds - elapsed))
                if (( remaining < 0 )); then remaining=0; fi
                describe_step "[$progress_label] 실행 중: 경과=${elapsed}초, 남은 시간=${remaining}초"
            else
                latest=""
                if [[ -s "$progress_log" ]]; then latest="$(tail -n 1 -- "$progress_log")"; fi
                describe_step "[$progress_label] 실행 중: 경과=$((SECONDS - started))초, PID=$job_pid, 최근 기록=${latest:-로그 대기 중}"
            fi
            # 생산기와 삭제기는 서로 다른 PID와 로그로 상태를 표시한다.
            if [[ -n "$delete_pid" ]]; then
                latest=""
                if [[ -s "$delete_log" ]]; then latest="$(tail -n 1 -- "$delete_log")"; fi
                describe_step "[STEP 2 삭제기 상태] 프로세스 실행 중: PID=$delete_pid, 조회=${SCAN_INTERVAL_MS} ms, 방식=$DELETION_MODE, 최근 기록=${latest:-로그 대기 중}"
            fi
            next_report=$((SECONDS + 1))
        fi
        sleep 1
    done
    wait "$job_pid" || status=$?
    job_pid=""
    if [[ "$status" -ne 0 ]]; then
        [[ -z "$progress_label" ]] || describe_step "[$progress_label] 실패: 경과=$((SECONDS - started))초, 종료 코드=$status, 상세 로그=$progress_log"
        fail "작업 실패: 종료 코드 $status"
    fi
    if [[ -n "$progress_label" ]]; then
        latest=""
        if [[ -s "$progress_log" ]]; then latest="$(tail -n 1 -- "$progress_log")"; fi
        if [[ -n "$duration_seconds" ]]; then
            describe_step "[$progress_label] 작업 종료: 경과=$((SECONDS - started))초, 남은 시간=0초"
            record_event "producer_exit_code=0 latest=${latest:-없음}"
        else
            describe_step "[$progress_label] 작업 종료: 경과=$((SECONDS - started))초, 종료 코드=0, 최근 기록=${latest:-없음}"
        fi
    fi
    if [[ -n "$delete_pid" ]]; then
        kill -0 "$delete_pid" 2>/dev/null || fail "실시간 삭제기가 비정상 종료되었습니다."
    fi
}
# 사용되는 기능의 의존성만 확인하고 Java 클래스는 실행별 디렉터리에 컴파일한다.
prepare_daemon() {
    local setting
    # 잘못된 감시 설정은 Java의 기본값 대체에 맡기지 않고 실행 전에 거부한다.
    for setting in JMX_PORT SCAN_INTERVAL_MS SCAN_LOG_INTERVAL_MS INACTIVITY_CHECK_INTERVAL_MS INACTIVITY_THRESHOLD_MS CLEANUP_TIMEOUT_SECONDS; do
        [[ "${!setting}" =~ ^[1-9][0-9]{0,8}$ ]] || fail "$setting 값은 양의 정수여야 합니다."
    done
    [[ "$DELETION_MODE" == BATCH || "$DELETION_MODE" == IMMEDIATE ]] || fail "DELETION_MODE는 BATCH 또는 IMMEDIATE입니다."
    (( CLEANUP_TIMEOUT_SECONDS <= 86400 )) || fail "삭제 제한 시간은 86400초 이하입니다."
    [[ -f "$DAEMON_SOURCE" ]] || fail "Java 소스가 없습니다: $DAEMON_SOURCE"
    grep -q DELETE_TOPIC_PREFIX "$DAEMON_SOURCE" || fail "업데이트된 Java 소스가 필요합니다."
    grep -q CLEANUP_ONCE "$DAEMON_SOURCE" || fail "일회성 삭제 지원 Java 소스가 필요합니다."
    # 순차 suite는 전용 런타임 디렉터리를 재사용해 회차별 class 폴더를 만들지 않는다.
    local class_dir="${EXPERIMENT_CLASS_DIR:-$OUTPUT_DIR/classes_$RUN_ID}"
    mkdir -p "$class_dir"
    daemon_cp="$class_dir:$built_jar:$KAFKA_HOME/libs/*:$KAFKA_HOME/clients/build/libs/*:$KAFKA_HOME/core/build/dependant-libs-2.13.16/*"
    [[ -f "$SCRIPT_DIR/tools/JmxPreflight.java" ]] || fail "JMX 검사 소스가 없습니다: tools/JmxPreflight.java"
    javac -encoding UTF-8 -cp "$daemon_cp" -d "$class_dir" "$DAEMON_SOURCE" "$SCRIPT_DIR/tools/JmxPreflight.java"
}
# 삭제 ON일 때 실험 부하를 만들기 전에 전체 JMX/RMI 및 Kafka MBean 조회를 검사한다.
check_jmx() {
    local log="$OUTPUT_DIR/${tag:-$RUN_ID}_jmx_preflight.log" started=$SECONDS status=0
    record_event "stage=JMX_PREFLIGHT host=$JMX_HOST port=$JMX_PORT timeout_seconds=$DELETE_START_TIMEOUT_SECONDS"
    describe_step "STEP 0-J: JMX 사전 검사 — $JMX_HOST:$JMX_PORT, 전체 RMI 연결 및 Kafka MBean 조회, 제한=${DELETE_START_TIMEOUT_SECONDS}초"
    record_command JMX_PREFLIGHT_COMMAND setsid java -cp "$daemon_cp" JmxPreflight "$JMX_HOST" "$JMX_PORT"
    setsid java -cp "$daemon_cp" JmxPreflight "$JMX_HOST" "$JMX_PORT" >"$log" 2>&1 &
    job_pid=$!
    while kill -0 "$job_pid" 2>/dev/null; do
        if (( SECONDS - started >= DELETE_START_TIMEOUT_SECONDS )); then
            stop_group "$job_pid"
            job_pid=""
            fail "JMX 사전 검사 시간 초과. TRIM/fio/토픽 작업을 시작하지 않습니다. 로그=$log"
        fi
        describe_step "[STEP 0-J 검사 진행] 경과=$((SECONDS - started))초, 제한=${DELETE_START_TIMEOUT_SECONDS}초"
        sleep 1
    done
    wait "$job_pid" || status=$?
    job_pid=""
    if (( status != 0 )) || ! grep -qF '[JMX Preflight] READY' "$log"; then
        fail "JMX 사전 검사 실패(exit=$status). 브로커 java.rmi.server.hostname 및 RMI 포트를 확인하세요. 로그=$log"
    fi
    record_event 'stage=JMX_PREFLIGHT status=SUCCESS'
    describe_step '[STEP 0-J 완료] 전체 JMX/RMI 연결 및 Kafka MBean 조회 성공'
}
daemon_args() {
    # 첨부 run.sh의 위치 인수 순서를 유지한다.
    DAEMON_ARGS=("$JMX_HOST" "$JMX_PORT" "$BROKERS" "$SCAN_INTERVAL_MS"
        "$INACTIVITY_CHECK_INTERVAL_MS" "$DELETION_MODE" "$1"
        "$INACTIVITY_THRESHOLD_MS" "$SCAN_LOG_INTERVAL_MS")
}
start_delete() {
    local scope="$1" tag="$2" deadline started=$SECONDS latest
    daemon_args "$OUTPUT_DIR/${tag}_deletion.csv"
    delete_log="$OUTPUT_DIR/${tag}_delete.log"
    echo "시간 기반 삭제 시작: prefix=$scope inactivity=${INACTIVITY_THRESHOLD_MS}ms log=$delete_log"
    record_event 'stage=DELETE_START'
    record_command DELETE_COMMAND env CLEANUP_ONCE=false DELETE_TOPIC_PREFIX="$scope" setsid java -cp "$daemon_cp" KafkaJmxProducerBatchDaemon "${DAEMON_ARGS[@]}"
    CLEANUP_ONCE=false DELETE_TOPIC_PREFIX="$scope" setsid java -cp "$daemon_cp" \
        KafkaJmxProducerBatchDaemon "${DAEMON_ARGS[@]}" >"$delete_log" 2>&1 &
    delete_pid=$!
    deadline=$((SECONDS + DELETE_START_TIMEOUT_SECONDS))
    until grep -qF '[Daemon] READY' "$delete_log"; do
        kill -0 "$delete_pid" 2>/dev/null || fail "감시 시작 실패: $delete_log"
        (( SECONDS < deadline )) || fail "감시 시작 시간 초과: $delete_log"
        latest="$(tail -n 1 -- "$delete_log")"
        describe_step "[STEP 2 진행] 삭제기 준비 중: 경과=$((SECONDS - started))초/${DELETE_START_TIMEOUT_SECONDS}초, PID=$delete_pid, 최근 기록=${latest:-JMX 연결 및 초기 조회 대기}"
        sleep 1
    done
    describe_step "[STEP 2 완료] JMX 연결·초기 조회 완료: PID=$delete_pid, 경과=$((SECONDS - started))초. 생산 중에도 1초마다 최근 삭제기 기록을 표시합니다."
}
run_cleanup() {
    local scope="$1" tag="$2" progress_label="${3:-STEP 5 cleanup 진행}"
    if [[ "$TRANSIENT_MODE" == true ]]; then
        run_transient_reset "$tag" "$progress_label"
        return 0
    fi
    local cleanup_start="" cleanup_count=""
    if [[ "$mode" == experiment ]]; then cleanup_start="$START_INDEX"; cleanup_count="$planned_topics"; fi
    [[ -n "$scope" && "$scope" != __* ]] || fail "삭제할 실험 접두어가 필요합니다."
    daemon_args "$OUTPUT_DIR/${tag}_cleanup.csv"
    echo "토픽 정리: prefix=$scope"
    describe_step "  cleanup: brokers=$BROKERS, 대상 접두어=$scope, 전체 제한=${CLEANUP_TIMEOUT_SECONDS}초. 비활성 시간과 관계없이 최대 1000개씩 순차 삭제 후 토픽 목록에서 사라질 때까지 확인합니다."
    record_event 'stage=CLEANUP'
    record_command CLEANUP_COMMAND env CLEANUP_ONCE=true CLEANUP_TOPIC_PREFIX="$scope" CLEANUP_START_INDEX="$cleanup_start" CLEANUP_TOPIC_COUNT="$cleanup_count" CLEANUP_TIMEOUT_SECONDS="$CLEANUP_TIMEOUT_SECONDS" setsid java -cp "$daemon_cp" KafkaJmxProducerBatchDaemon "${DAEMON_ARGS[@]}"
    CLEANUP_ONCE=true CLEANUP_TOPIC_PREFIX="$scope" CLEANUP_START_INDEX="$cleanup_start" CLEANUP_TOPIC_COUNT="$cleanup_count" CLEANUP_TIMEOUT_SECONDS="$CLEANUP_TIMEOUT_SECONDS" \
        setsid java -cp "$daemon_cp" KafkaJmxProducerBatchDaemon "${DAEMON_ARGS[@]}" \
        >"$OUTPUT_DIR/${tag}_cleanup.log" 2>&1 &
    job_pid=$!
    wait_job "$progress_label" "$OUTPUT_DIR/${tag}_cleanup.log"
}
# 기본 reset은 apps의 AdminClient 앱을 사용하며 외부 스크립트로 재정의할 수 있다.
run_transient_reset() {
    local progress_label="$2" log="$OUTPUT_DIR/${1}_reset.log"
    [[ "$CLEANUP_ENABLED" == true ]] || fail "Transient cleanup이 비활성화되어 있습니다."
    [[ -z "$job_pid" ]] || fail "생산 작업 종료 전에는 Transient reset을 실행할 수 없습니다."
    local -a reset_args=(--bootstrap-server "$BROKERS" --timeout-ms "$((CLEANUP_TIMEOUT_SECONDS * 1000))")
    local -a reset_command=(bash "$RUNNER" apps ResetTransientTopics "${reset_args[@]}")
    if [[ -n "$TRANSIENT_RESET_SCRIPT" ]]; then
        reset_command=(bash "$TRANSIENT_RESET_SCRIPT" "${reset_args[@]}")
    fi
    describe_step "  Transient reset: 모든 Transient 클라이언트가 종료된 실험 클러스터 전체를 초기화합니다. 제한=${CLEANUP_TIMEOUT_SECONDS}초, 로그=$log"
    record_event 'stage=TRANSIENT_RESET_START scope=cluster'
    record_command TRANSIENT_RESET_COMMAND setsid "${reset_command[@]}"
    transient_reset_attempted=true
    setsid "${reset_command[@]}" >"$log" 2>&1 &
    job_pid=$!
    wait_job "$progress_label" "$log" "" "$CLEANUP_TIMEOUT_SECONDS"
    record_event 'stage=TRANSIENT_RESET_SUCCESS scope=cluster'
}
run_fio() {
    local fio_log="$OUTPUT_DIR/${1}_fio.log"
    local -a fio_command=(bash "$FIO_SCRIPT")
    if [[ -n "$BROKER_SSH" ]]; then fio_command=("${remote_command[@]}" --run "${RUN_ID}_fio" fio); fi
    echo "fio 사전 준비: target=$FIO_TARGET_DIR"
    echo "fio 진행 출력과 로그: $fio_log"
    if [[ "$FIO_SIZE_GIB" == auto ]]; then
        describe_step "  fio 용량: $FIO_TARGET_DIR 의 가용 공간에서 ${RESERVE_GIB} GiB를 남긴 나머지를 사용합니다. 실제 계산 용량은 아래 fio 출력에서 확인합니다."
    else
        describe_step "  fio 용량: ${FIO_SIZE_GIB} GiB 파일을 사용하며 예약 공간 ${RESERVE_GIB} GiB 확보 여부를 확인합니다."
    fi
    describe_step "  fio 작업: 예상 장치=$EXPECTED_DISK, 총 쓰기=${FIO_PASSES}회(첫 채우기 포함), 대상=$FIO_TARGET_DIR. 완료 후 임시 파일을 삭제해야 다음 단계로 진행합니다."
    record_event 'stage=FIO'
    record_command FIO_COMMAND setsid "${fio_command[@]}" --target-dir "$FIO_TARGET_DIR" --size-gib "$FIO_SIZE_GIB" --passes "$FIO_PASSES" --reserve-gib "$RESERVE_GIB" --expected-disk "$EXPECTED_DISK"
    setsid "${fio_command[@]}" --target-dir "$FIO_TARGET_DIR" --size-gib "$FIO_SIZE_GIB" \
        --passes "$FIO_PASSES" --reserve-gib "$RESERVE_GIB" --expected-disk "$EXPECTED_DISK" \
        > >(tee "$fio_log") 2>&1 &
    job_pid=$!
    wait_job
}
# 빌드는 실험 루프 밖에서 호출당 한 번 완료한다. 빌드 실패는 실험 실패 복구 대상이 아니다.
build_producer_jar() {
    local build_started=$SECONDS build_status=0
    record_file="$build_metadata"
    printf 'BUILD_ENABLED=%s\n' "$BUILD_ENABLED" >> "$build_metadata"
    if [[ "$BUILD_ENABLED" == false ]]; then
        record_event 'stage=BUILD status=SKIPPED reason=no-build'
        printf '[BUILD] 생략 (--no-build): 기존 JAR 사용=%s\n' "$built_jar" | tee -a "$build_log"
        # 파일이 없으면 실험/cleanup을 시작하지 않는다. 소스와 JAR의 최신성은 자동 판정하지 않는다.
        [[ -s "$built_jar" && -r "$built_jar" ]] || fail "사용 가능한 기존 JAR이 없습니다: $built_jar (--build로 빌드하세요)"
        if command -v sha256sum >/dev/null; then sha256sum -- "$built_jar" >> "$build_metadata"; fi
        record_event 'stage=JAR_REUSE status=SUCCESS'
        record_file=""
        return 0
    fi
    if [[ -n "$BROKER_SSH" ]]; then
        record_event 'stage=BUILD status=STARTED execution=remote'
        record_command BUILD_COMMAND "${remote_command[@]}" --run "${RUN_ID}_build" --jar-destination "$built_jar" build
        setsid "${remote_command[@]}" --run "${RUN_ID}_build" --jar-destination "$built_jar" build >"$build_log" 2>&1 &
        job_pid=$!
        while kill -0 "$job_pid" 2>/dev/null; do
            printf '[BUILD] 원격 진행: 서버=%s, 경과=%s초\n' "$BROKER_SSH" "$((SECONDS - build_started))"
            sleep 1
        done
        wait "$job_pid" || build_status=$?
        job_pid=""
        [[ "$build_status" == 0 && -s "$built_jar" ]] || fail "원격 빌드/JAR 전달 실패: $build_log"
        sha256sum "$built_jar" >> "$build_metadata"
        record_event 'stage=BUILD status=SUCCESS execution=remote jar_sync=verified'
        record_file=""
        return 0
    fi
    [[ -f "$CLIENT_DIR/gradlew" && -f "$CLIENT_DIR/settings.gradle" ]] || fail "빌드 가능한 프로젝트가 필요합니다: --client-dir에 gradlew/settings.gradle이 있는 경로를 지정하세요."
    CLIENT_DIR="$(cd "$CLIENT_DIR" && pwd)"
    built_jar="$CLIENT_DIR/build/libs/apps-1.0-SNAPSHOT-all.jar"
    record_file="$build_metadata"
    record_event 'stage=BUILD status=STARTED'
    record_command BUILD_COMMAND bash "$CLIENT_DIR/gradlew" -p "$CLIENT_DIR" :apps:shadowJar --rerun-tasks --no-build-cache --no-daemon --console=plain
    printf '[BUILD] 시작\n' | tee -a "$build_log"
    # UP-TO-DATE/빌드 캐시 재사용을 막고 의존 태스크도 다시 실행한다. 출력 디렉터리 clean은 하지 않는다.
    setsid bash "$CLIENT_DIR/gradlew" -p "$CLIENT_DIR" :apps:shadowJar \
        --rerun-tasks --no-build-cache --no-daemon --console=plain \
        >/dev/null 2>&1 &
    job_pid=$!
    while kill -0 "$job_pid" 2>/dev/null; do
        printf '[BUILD] 진행 중: 경과=%s초\n' "$((SECONDS - build_started))" | tee -a "$build_log"
        sleep 1
    done
    wait "$job_pid" || build_status=$?
    job_pid=""
    printf '[BUILD] 종료: 경과=%s초, 종료 코드=%s\n' "$((SECONDS - build_started))" "$build_status" | tee -a "$build_log"
    [[ "$build_status" -eq 0 ]] || fail "JAR 빌드 실패: 종료 코드=$build_status (요청에 따라 상세 출력은 저장하지 않음)"
    [[ -s "$built_jar" ]] || fail "빌드 완료 후 JAR을 찾지 못했습니다: $built_jar"
    if command -v sha256sum >/dev/null; then sha256sum -- "$built_jar" >> "$build_metadata"; fi
    record_event 'stage=BUILD status=SUCCESS'
    record_file=""
}
build_producer_jar
if [[ "$mode" == experiment && "$TRANSIENT_MODE" == true ]]; then
    # --no-build/원격 빌드의 JAR에 필요한 클래스가 있는지 장시간 준비 전에 확인한다.
    record_file="$build_metadata"
    "$PYTHON" - "$built_jar" "$CLEANUP_ENABLED" "$TRANSIENT_RESET_SCRIPT" <<'PY' || fail "Transient 생산기/reset 앱과 커스텀 Kafka API를 포함한 apps JAR이 필요합니다. --build와 CUSTOM_KAFKA_PATH를 확인하세요."
import sys
import zipfile

required = {
    'org/example/apps/AutoTransientTopicProducerMetrics.class',
    'org/apache/kafka/clients/producer/KafkaTransientProducer.class',
    'org/apache/kafka/clients/producer/TransientProducerRecord.class',
    'org/apache/kafka/clients/producer/TransientTopicHandle.class',
    'org/apache/kafka/clients/producer/TransientCallback.class',
}
if sys.argv[2] == 'true' and not sys.argv[3]:
    required.update({
        'org/example/apps/ResetTransientTopics.class',
        'org/apache/kafka/clients/admin/ResetTransientTopicsOptions.class',
        'org/apache/kafka/clients/admin/ResetTransientTopicsResult.class',
    })
with zipfile.ZipFile(sys.argv[1]) as jar:
    missing = required.difference(jar.namelist())
if missing:
    sys.exit('JAR에 필요한 클래스가 없습니다: ' + ', '.join(sorted(missing)))
PY
    record_event 'stage=TRANSIENT_JAR_CHECK status=SUCCESS'
    record_file=""
fi
if [[ -z "$BROKER_SSH" && ( "$mode" == fio || ( "$mode" == experiment && "$FIO_ENABLED" == true ) ) ]]; then
    [[ -f "$FIO_SCRIPT" ]] || fail "fio 스크립트가 없습니다: $FIO_SCRIPT"
fi
if [[ "$mode" == delete || "$mode" == cleanup || ( "$mode" == experiment && "$TRANSIENT_MODE" == false ) ]]; then
    # 정상 cleanup OFF여도 실패 복구용 cleanup은 항상 준비한다.
    prepare_daemon
fi
case "$mode" in
    fio) start_record "$RUN_ID" n/a; run_fio "$RUN_ID"; finish_record; exit 0 ;;
    delete)
        start_record "$RUN_ID" n/a
        [[ -n "${DELETE_TOPIC_PREFIX:-}" && "$DELETE_TOPIC_PREFIX" != __* ]] || fail "DELETE_TOPIC_PREFIX를 지정하세요."
        check_jmx
        start_delete "$DELETE_TOPIC_PREFIX" "$RUN_ID"
        # 단독 감시는 사용자가 종료할 때까지 유지한다.
        wait "$delete_pid"
        fail "감시기가 종료되었습니다."
        ;;
    cleanup)
        start_record "$RUN_ID" n/a
        run_cleanup "${CLEANUP_TOPIC_PREFIX:?삭제할 토픽 접두어를 지정하세요}" "$RUN_ID"
        finish_record
        exit 0 ;;
esac
[[ -f "$RUNNER" ]] || fail "생산 실행기가 없습니다: $RUNNER"
# 생산 수치 오류를 장시간 fio 실행 전에 확인한다.
for setting in RUN_DURATION_MINUTES PRODUCE_HZ_PER_TOPIC; do
    [[ "${!setting}" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "$setting 값은 양수여야 합니다."
    awk -v value="${!setting}" 'BEGIN {exit !(value > 0)}' || fail "$setting 값은 양수여야 합니다."
done
for setting in TOPIC_LIFETIME_MS MSG_SIZE; do
    [[ "${!setting}" =~ ^[1-9][0-9]*$ ]] || fail "$setting 값은 양의 정수여야 합니다."
done
for setting in START_INDEX START_BARRIER_DELAY_MS; do
    [[ "${!setting}" =~ ^[0-9]+$ ]] || fail "$setting 값은 0 이상의 정수여야 합니다."
done
[[ "$TOPIC_CNT" =~ ^-?[0-9]+$ ]] || fail "TOPIC_CNT는 정수여야 합니다."
# 자동 관리하는 접두어·주파수·출력 경로의 중복 지정으로 실험 범위가 달라지는 것을 막는다.
for arg in "$@"; do
    case "$arg" in
        --check-topics-absent*|--client-index*|--start-index*|--topic-cnt*|--run-duration-minutes*|--prepare-topics-only|--prepare-topics-only=*|--require-existing-topics|--require-existing-topics=*|-p|-p?*|--prefix|--prefix=*|--topic-create-hz|--topic-create-hz=*|--metrics-file|--metrics-file=*|--produce-stats-file|--produce-stats-file=*)
            fail "자동 관리 옵션입니다: $arg (PREFIX 등 환경 변수로 설정하세요)" ;;
    esac
    if [[ "$TOPIC_MODE" == precreate ]]; then
        case "$arg" in
            --client-index*|--start-index*|--topic-cnt*|--run-duration-minutes*|--topic-prepare-timeout-seconds*)
                fail "precreate에서는 토픽 이름/수/준비 설정을 추가 인수로 덮어쓸 수 없습니다: $arg" ;;
        esac
    fi
done
for hz in "${rates[@]}"; do
    awk -v hz="$hz" 'BEGIN {exit !(hz > 0 && hz <= 20)}' || fail "Hz 범위는 0 초과 20 이하입니다."
done
# 각 Hz를 N회 완료한 뒤 다음 Hz로 이동한다 (1,1,2,2,...).
total_experiments=$((${#rates[@]} * RETRY))
failed_experiments=0
for ((index=0; index<total_experiments; index++)); do
    rate_index=$((index / RETRY))
    retry_index=$((index % RETRY + 1))
    hz="${rates[rate_index]}"
    tag="${RUN_ID}_${rate_index}_${hz}Hz"
    if (( RETRY > 1 )); then tag="${tag}_repeat${retry_index}"; fi
    # 모든 Hz/반복에서 같은 토픽 이름을 재사용하며 결과 파일만 고유 태그로 분리한다.
    experiment_prefix="$PREFIX"
    metrics_file="$OUTPUT_DIR/${tag}_metrics.csv"
    full_file="$OUTPUT_DIR/${tag}_full.csv"
    # 조건문으로 감싸면 errexit가 무효화되므로 독립 서브셸로 실행한다.
    set +e
    (
    set -Eeuo pipefail
    experiment_active=true
    # 회차별 폴더는 고유 실행 ID·Hz·반복·실제 시작 시각으로 구분한다.
    round_start_iso="$(TZ=KST-9 date +%Y-%m-%dT%H:%M:%S+09:00)"
    round_start_stamp="$(TZ=KST-9 date +%Y%m%d_%H%M%S)"
    fio_label=off
    [[ "$FIO_ENABLED" != true ]] || fio_label=on
    delete_label=off
    [[ "$DELETE_ENABLED" != true ]] || delete_label="on-${DELETION_MODE,,}"
    fio_count=0
    [[ "$FIO_ENABLED" != true ]] || fio_count="$FIO_PASSES"
    round_directory="${EXPERIMENT_LABEL}_${hz}Hz_${TOPIC_MODE}_fio${fio_count}_del-${delete_label}_scan${SCAN_INTERVAL_MS}_check${INACTIVITY_CHECK_INTERVAL_MS}_${RUN_DURATION_MINUTES}min_repeat${retry_index}_${RUN_ID}_start_${round_start_stamp}_KST"
    if [[ "$OUTPUT_LAYOUT" == nested ]]; then
        OUTPUT_DIR="$OUTPUT_DIR/$round_directory"
        mkdir -- "$OUTPUT_DIR"
    fi
    tag="${round_directory}_end_PENDING"
    metrics_file="$OUTPUT_DIR/${tag}_metrics.csv"
    full_file="$OUTPUT_DIR/${tag}_full.csv"
    round_started=true
    experiment_cleanup_allowed=false
    previous_step=""
    cache_before_done=false
    transient_reset_attempted=false
    interrupted=false
    record_file="$OUTPUT_DIR/${tag}.run.txt"
    trap on_exit EXIT
    trap 'interrupted=true; exit 130' INT
    trap 'interrupted=true; exit 143' TERM
    trap 'interrupted=true; exit 129' HUP
    trap 'error_status=$?; record_event "failure_exit_code=$error_status line=$LINENO command=$BASH_COMMAND"; printf "ERROR exit_code=%s line=%s command=%s\n" "$error_status" "$LINENO" "$BASH_COMMAND" >> "${record_file%.run.txt}_failure.log"' ERR
    start_record "$tag" "$hz"
    planned_topics="$(awk -v n="$TOPIC_CNT" -v hz="$hz" -v minutes="$RUN_DURATION_MINUTES" 'BEGIN {v=hz*60*minutes; count=int(v)+(v>int(v)); if(count<1)count=1; printf "%.0f", (n>0?n:count)}')"
    describe_step "실험 $((index + 1))/${total_experiments}: ${hz} Hz, 반복=${retry_index}/${RETRY}, 토픽 접두어=$experiment_prefix"
    # 검사가 실패하면 cleanup 허용 전이므로 기존 토픽을 건드리지 않고 전체 중단한다.
    if [[ "$DELETE_ENABLED" == true ]]; then
        begin_step JMX_PREFLIGHT
        check_jmx
    fi
    if [[ "$DROP_CACHES" == true ]]; then
        begin_step CACHE_BEFORE
        drop_caches before
        cache_before_done=true
    fi
    if [[ "$TOPIC_MODE" == precreate ]]; then
        begin_step PRECREATE_CLEANUP
        # precreate는 해당 회차의 정확한 이름 범위만 먼저 정리한다. 다른 인덱스/접두어는 보존한다.
        # 삭제 실패 또는 잔존 토픽 발견 시 사전 생성/생산 없이 전체 실행을 중단한다.
        describe_step "STEP 0-C: 시작 전 정리 — ${experiment_prefix}_0_${START_INDEX}부터 ${planned_topics}개 토픽과 데이터를 삭제하고 삭제 완료를 확인합니다."
        record_event "stage=PRECREATE_CLEANUP_START start_index=$START_INDEX topic_count=$planned_topics"
        run_cleanup "${experiment_prefix}_0_" "${tag}_before_precreate" 'STEP 0-C 시작 전 cleanup 진행'
        record_event 'stage=PRECREATE_CLEANUP_SUCCESS'
    fi
    if [[ "$TRANSIENT_MODE" == true ]]; then
        describe_step 'STEP 0: Transient 모드 — UUID로 토픽을 생성하므로 일반 토픽 이름 충돌 검사를 생략합니다.'
        record_event 'stage=TOPIC_ABSENCE_CHECK status=SKIPPED reason=transient'
    else
        begin_step TOPIC_ABSENCE_CHECK
        record_event 'stage=TOPIC_ABSENCE_CHECK'
        check_args=(apps AutoTopicProducerMetrics -b "$BROKERS" -p "$experiment_prefix"
            --client-index 0 --start-index "$START_INDEX" --topic-cnt "$planned_topics"
            --check-topics-absent --topic-prepare-timeout-seconds "$TOPIC_PREPARE_TIMEOUT_SECONDS")
        record_command TOPIC_ABSENCE_COMMAND setsid bash "$RUNNER" "${check_args[@]}"
        setsid bash "$RUNNER" "${check_args[@]}" >"$OUTPUT_DIR/${tag}_topic_check.log" 2>&1 &
        job_pid=$!
        wait_job 'STEP 0 토픽 충돌 검사' "$OUTPUT_DIR/${tag}_topic_check.log"
        experiment_cleanup_allowed=true
    fi
    # fio가 만든 부하 상태를 유지하기 위해 TRIM은 사전 생성/fio보다 앞에서 실행한다.
    if [[ "$FSTRIM_ENABLED" == true ]]; then
        begin_step FSTRIM
        run_fstrim
    else
        describe_step "STEP 0-T: fstrim 및 대기 생략 (--no-fstrim)."
    fi
    if [[ "$TOPIC_MODE" == precreate ]]; then
        begin_step TOPIC_PRECREATE
        describe_step "STEP 0-P: 토픽 ${planned_topics}개 생성·리더/ISR 확인·토픽당 ${MSG_SIZE}바이트 준비 메시지 1개 전송(응답 확인) → fio=$FIO_ENABLED → 본 실험 생산 → cleanup. 준비 전송은 생산 CSV에서 제외합니다."
        prepare_args=(apps AutoTopicProducerMetrics -b "$BROKERS" -p "$experiment_prefix"
            --client-index 0 --start-index "$START_INDEX" --topic-cnt "$planned_topics"
            --prepare-topics-only --msg-size "$MSG_SIZE" --topic-prepare-timeout-seconds "$TOPIC_PREPARE_TIMEOUT_SECONDS")
        record_event 'stage=TOPIC_PRECREATE'
        record_command TOPIC_PRECREATE_COMMAND setsid bash "$RUNNER" "${prepare_args[@]}"
        setsid bash "$RUNNER" "${prepare_args[@]}" >"$OUTPUT_DIR/${tag}_precreate.log" 2>&1 &
        job_pid=$!
        wait_job 'STEP 0-P 토픽 준비' "$OUTPUT_DIR/${tag}_precreate.log"
    fi
    if [[ "$IOSTAT_ENABLED" == true ]]; then
        describe_step "STEP 0: iostat ON — 1초 간격으로 fio·생산·cleanup·대기 구간을 기록합니다. 로그=$OUTPUT_DIR/${tag}_iostat.log"
    else
        describe_step "STEP 0: iostat OFF — 디스크 상태 로그를 수집하지 않습니다 (--no-iostat)."
    fi
    if [[ "$FIO_ENABLED" == true ]]; then
        begin_step FIO
        describe_step "STEP 1: fio 사전 준비 ON — 스크립트=$FIO_SCRIPT"
    else
        describe_step "STEP 1: fio 사전 준비 생략 (--no-fio). SSD 준비 쓰기를 하지 않고 다음 단계로 진행합니다."
    fi
    [[ "$FIO_ENABLED" == false ]] || run_fio "$tag"
    if [[ "$DELETE_ENABLED" == true ]]; then
        begin_step DELETE_START
        describe_step "STEP 2: 시간 기반 삭제 ON — 마지막 오프셋 증가 관측 후 ${INACTIVITY_THRESHOLD_MS} ms 이상 비활성이면 삭제합니다."
        describe_step "  조회=${SCAN_INTERVAL_MS} ms, 비활성 검사=${INACTIVITY_CHECK_INTERVAL_MS} ms, 로그 간격=${SCAN_LOG_INTERVAL_MS} ms, 삭제 방식=$DELETION_MODE"
        describe_step "  대상 접두어=$DELETE_TOPIC_PREFIX, JMX=$JMX_HOST:$JMX_PORT, brokers=$BROKERS, 시작 제한=${DELETE_START_TIMEOUT_SECONDS}초"
        describe_step "  삭제 결과=$OUTPUT_DIR/${tag}_deletion.csv, 데몬 로그=$OUTPUT_DIR/${tag}_delete.log"
    else
        describe_step "STEP 2: 시간 기반 삭제 생략 (--no-delete). 생산 중 비활성 토픽을 삭제하지 않으며 조회 주기·비활성 기준·삭제 방식은 적용하지 않습니다. 종료 후 cleanup 설정은 별도입니다."
    fi
    [[ "$DELETE_ENABLED" == false ]] || start_delete "$DELETE_TOPIC_PREFIX" "$tag"
    begin_step PRODUCER
    planned_topics="$(awk -v n="$TOPIC_CNT" -v hz="$hz" -v minutes="$RUN_DURATION_MINUTES" 'BEGIN {v=hz*60*minutes; count=int(v)+(v>int(v)); if(count<1)count=1; printf "%.0f", (n>0?n:count)}')"
    describe_step "STEP 3: 생산 시작 — ${hz} Hz로 작업자를 시작하고 토픽당 ${PRODUCE_HZ_PER_TOPIC} msg/s, 메시지 ${MSG_SIZE} bytes, 토픽당 생산 ${TOPIC_LIFETIME_MS} ms를 적용합니다."
    describe_step "  시간 설정=${RUN_DURATION_MINUTES}분, topic-cnt=$TOPIC_CNT → 계산 토픽 수=$planned_topics, 시작 인덱스=$START_INDEX, 시작 배리어=${START_BARRIER_DELAY_MS} ms"
    describe_step "  시간 설정은 토픽 수 계산용입니다. 실제 종료에는 마지막 작업자의 생산·전송 마무리 시간이 추가됩니다. brokers=$BROKERS"
    describe_step "  남은 시간은 설정된 ${RUN_DURATION_MINUTES}분 기준이며 실제 종료 예상 시간이 아닙니다. 0초 이후에도 생산이 끝날 때까지 기다립니다."
    describe_step "  생산 로그=$OUTPUT_DIR/${tag}_producer.log, 설정 기록=$record_file"
    if [[ "$TOPIC_MODE" == transient-precreate ]]; then
        describe_step "  Transient 사전 생성 ON: 토픽 ${planned_topics}개 생성 응답을 모두 받은 뒤 생산하고, 모든 워커가 끝나면 전체 seal합니다. 전체 준비 제한=${TOPIC_PREPARE_TIMEOUT_SECONDS}초."
    fi
    if [[ "$STATS_DISABLED" == true ]]; then
        describe_step "  생산 통계 OFF (--stats-disabled). 생산 CSV 생성 확인을 생략합니다."
    else
        describe_step "  생산 통계 ON. 저장 예정 CSV=$metrics_file"
    fi
    if [[ $# -gt 0 ]]; then
        printf -v extra_display '%q ' "$@"
        describe_step "  생산기에 추가 전달하는 인수: $extra_display (위 기본 설정과 중복되는 값은 실제 전달 명령도 확인하세요.)"
    fi
    args=(apps "$PRODUCER_CLASS" -b "$BROKERS" -p "$experiment_prefix"
        --start-index "$START_INDEX" --topic-cnt "$TOPIC_CNT" --topic-create-hz "$hz"
        --run-duration-minutes "$RUN_DURATION_MINUTES" --topic-lifetime-ms "$TOPIC_LIFETIME_MS"
        --produce-hz-per-topic "$PRODUCE_HZ_PER_TOPIC" --msg-size "$MSG_SIZE"
        --start-barrier-delay "$START_BARRIER_DELAY_MS" --metrics-file "$metrics_file")
    [[ "$STATS_DISABLED" == false ]] || args+=(--stats-disabled)
    if [[ "$TOPIC_MODE" == precreate ]]; then
        args+=(--require-existing-topics --topic-prepare-timeout-seconds "$TOPIC_PREPARE_TIMEOUT_SECONDS")
    fi
    if [[ "$TRANSIENT_MODE" == true ]]; then
        args+=(--full-file "$full_file")
    fi
    if [[ "$TOPIC_MODE" == transient-precreate ]]; then
        args+=(--precreate --topic-prepare-timeout-seconds "$TOPIC_PREPARE_TIMEOUT_SECONDS")
    fi
    record_event 'stage=PRODUCER'
    record_command PRODUCER_COMMAND setsid bash "$RUNNER" "${args[@]}" "$@"
    # 생산기 실행부터는 사전 생성 중 일부 토픽이 생길 수 있으므로 실패 시 reset 대상으로 둔다.
    experiment_cleanup_allowed=true
    setsid bash "$RUNNER" "${args[@]}" "$@" >"$OUTPUT_DIR/${tag}_producer.log" 2>&1 &
    job_pid=$!
    # 삭제 기능 OFF일 때도 생산 경과를 표시하며, ON이면 삭제 상태를 함께 표시한다.
    wait_job 'STEP 3 생산 진행' "$OUTPUT_DIR/${tag}_producer.log" "$(awk -v minutes="$RUN_DURATION_MINUTES" 'BEGIN {v=minutes*60; printf "%.0f", int(v)+(v>int(v))}')"
    begin_step RESULT_CHECK
    if [[ "$DELETE_ENABLED" == true ]]; then
        describe_step "STEP 4: 생산 프로세스 종료. 실행 중인 시간 기반 삭제기를 종료하고 결과를 확인합니다."
    else
        describe_step "STEP 4: 생산 프로세스 종료. 삭제기는 실행하지 않았으므로 종료할 삭제기 없이 결과만 확인합니다."
    fi
    stop_group "$delete_pid" 'STEP 4 진행'
    delete_pid=""
    # Java CLI가 오류 종료 코드를 반환하지 않는 경우도 결과 파일로 확인한다.
    if [[ "$STATS_DISABLED" == false ]]; then
        describe_step "[STEP 4 진행] 생산 CSV 확인 중: $metrics_file"
        [[ -s "$metrics_file" ]] || fail "생산 결과 CSV가 없습니다: $metrics_file"
        csv_bytes="$(wc -c < "$metrics_file")"
        describe_step "[STEP 4 완료] 생산 CSV 확인 성공: ${csv_bytes} bytes, $metrics_file"
    else
        describe_step "[STEP 4 완료] 통계 OFF이므로 CSV 검사를 생략했습니다."
    fi
    # cleanup 시간과 대기 시간을 합쳐 최소 inter-experiment-delay-seconds를 확보한다.
    # 단일/마지막 실험도 cleanup을 켰으면 같은 정리 구간을 적용한다.
    if [[ "$CLEANUP_ENABLED" == true || "$TRANSIENT_MODE" == true ]] || (( index + 1 < total_experiments )); then
        begin_step CLEANUP
        cooldown_started=$SECONDS
        record_event "stage=COOLDOWN_START requested_seconds=$INTER_EXPERIMENT_DELAY_SECONDS"
        describe_step "STEP 5: 정리·대기 시작 — cleanup=$CLEANUP_ENABLED, 정리 시작부터 최소 ${INTER_EXPERIMENT_DELAY_SECONDS}초. cleanup 시간은 이 구간에 포함됩니다."
        if [[ "$CLEANUP_ENABLED" == false ]]; then
            describe_step "  cleanup 생략 (--no-cleanup). 설정된 회차 간 대기만 수행합니다."
        else
            describe_step "  cleanup은 단일/마지막 Hz에도 적용합니다. 정리가 ${INTER_EXPERIMENT_DELAY_SECONDS}초를 초과하면 완료까지 기다리고, 삭제 제한 ${CLEANUP_TIMEOUT_SECONDS}초를 넘기면 중단합니다."
        fi
        [[ "$CLEANUP_ENABLED" == false ]] || run_cleanup "${experiment_prefix}_0_" "$tag"
        cooldown_deadline=$((cooldown_started + 10#$INTER_EXPERIMENT_DELAY_SECONDS))
        remaining_seconds=$((cooldown_deadline - SECONDS))
        (( remaining_seconds >= 0 )) || remaining_seconds=0
        describe_step "  정리 단계 경과=$((SECONDS - cooldown_started))초, 남은 대기=${remaining_seconds}초. iostat enabled=$IOSTAT_ENABLED"
        while (( SECONDS < cooldown_deadline )); do
            if [[ -n "$iostat_pid" ]]; then
                kill -0 "$iostat_pid" 2>/dev/null || fail "정리 대기 중 iostat이 종료되었습니다."
            fi
            describe_step "[STEP 5 대기] cleanup 이후 대기 중: 전체 경과=$((SECONDS - cooldown_started))초, 남은 시간=$((cooldown_deadline - SECONDS))초, iostat=$IOSTAT_ENABLED"
            sleep 1
        done
        record_event "stage=COOLDOWN_END elapsed_seconds=$((SECONDS - cooldown_started))"
        describe_step "  정리·대기 완료: 실제 경과=$((SECONDS - cooldown_started))초"
    else
        describe_step "STEP 5: cleanup OFF이며 마지막/단일 Hz이므로 정리·대기를 생략합니다. 설정된 딜레이 ${INTER_EXPERIMENT_DELAY_SECONDS}초는 적용하지 않습니다."
    fi
    if [[ "$DROP_CACHES" == true ]]; then
        begin_step CACHE_AFTER
        drop_caches after
    fi
    if (( index + 1 < total_experiments )); then
        describe_step "STEP 6: ${hz} Hz 반복 ${retry_index}/${RETRY} 기록 종료. 다음 실험=${rates[(index + 1) / RETRY]} Hz, 반복=$(((index + 1) % RETRY + 1))/${RETRY}. iostat enabled=$IOSTAT_ENABLED (켜진 경우 현재 수집을 종료하고 다음 실험에서 새 로그를 시작합니다)."
    else
        describe_step "STEP 6: 마지막 ${hz} Hz 기록 종료. 결과 디렉터리=$OUTPUT_DIR"
    fi
    finish_record
    )
    experiment_status=$?
    set -e
    case "$experiment_status" in
        0) ;;
        20) failed_experiments=$((failed_experiments + 1))
            echo "실험 실패 후 cleanup 완료: ${tag}. 남은 실험을 계속합니다." ;;
        *) fail "실험 복구 실패 또는 사용자 중단: ${tag} (종료 코드=$experiment_status). 다음 실험을 실행하지 않습니다." ;;
    esac
done
echo "전체 실행 완료: 총 ${total_experiments}회, 실패 ${failed_experiments}회. 결과=$OUTPUT_DIR"
(( failed_experiments == 0 )) || exit 1
