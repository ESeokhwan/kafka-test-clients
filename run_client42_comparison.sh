#!/usr/bin/env bash
# .42에서 삭제 OFF/BATCH/IMMEDIATE × fio OFF/ON의 여섯 조합을 실행한다.
set -Eeuo pipefail
base="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
original=("$@")
variant=all
selected=false
deletion_filter=""
interval_selector=""
scan_ms=1000
check_ms=1000
manual_interval=false
selection=(--all)
selection_seen=false
output="$base/experiment_results/client42_delete_fio"
forwarded=()
usage() {
    printf '%s\n' \
      '사용법: bash run_client42_comparison.sh [--variant all|delete_{off,batch,immediate}_fio_{off,on}] [옵션]' \
      '기본: 여섯 조합 순차 실행 / 각 조합 1~5Hz / 각 Hz 총 3회 / 회차당 20분 / auto 모드' \
      '선택: --hz 3 또는 --all, --retry 3, --run-duration-minutes 20, --output-dir ROOT' \
      '간격: --intervals all 또는 1000,300000처럼 쉼표로 선택. 허용 값 1000/30000/60000/300000ms.' \
      '생략 시 기존 1000ms. --intervals는 개별 scan/inactivity-check 옵션과 함께 사용할 수 없음.' \
      '삭제: --deletion-mode BATCH|IMMEDIATE는 all에서 해당 삭제 방식만 선택(OFF 포함 4조합). 생략 시 두 방식 모두.' \
      '삭제 기준: --scan-interval-ms 1000 --inactivity-threshold-ms 600000. 기존 delete_on 이름은 기본 BATCH 별칭.' \
      'fio: --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 --reserve-gib 2 --expected-disk nvme1n1' \
      'TRIM 기본 ON: --fstrim-wait-seconds 600 (최소 600초), --no-fstrim으로 생략' \
      'build/cache drop/iostat 기본 OFF; --build/--drop-caches/--iostat으로 변경 가능' \
      'delete/fio/cleanup/topic-mode는 케이스가 고정한다. 오류 또는 중단 시 다음 조합은 실행하지 않는다.'
}
fail() { printf '오류: %s\n' "$*" >&2; exit 2; }
while (( $# )); do
    option="${1%%=*}"
    case "$option" in
        --help|-h) usage; exit 0 ;;
        --all)
            [[ "$1" == --all && "$selection_seen" == false ]] || fail 'Hz 선택은 한 번만 지정하세요.'
            selection=(--all); selection_seen=true; shift; continue ;;
        --build|--no-build|--drop-caches|--no-drop-caches|--iostat|--no-iostat|--fstrim|--no-fstrim|--stats-enabled|--stats-disabled)
            [[ "$1" != *=* ]] || fail "$option 은 값을 받지 않습니다."
            forwarded+=("$1"); shift; continue ;;
        --intervals|--variant|--hz|--output-dir|--retry|--run-duration-minutes|--prefix|--topic-cnt|--topic-lifetime-ms|--produce-hz-per-topic|--msg-size|--start-index|--start-barrier-delay-ms|--topic-prepare-timeout-seconds|--step-delay-seconds|--inter-experiment-delay-seconds|--fstrim-target-dir|--fstrim-wait-seconds|--fio-target-dir|--fio-size-gib|--fio-passes|--reserve-gib|--expected-disk|--deletion-mode|--scan-interval-ms|--scan-log-interval-ms|--inactivity-check-interval-ms|--inactivity-threshold-ms|--delete-start-timeout-seconds|--cleanup-timeout-seconds|--jmx-host|--jmx-port|--remote-identity-file|--remote-stage-timeout-seconds) ;;
        *) fail "지원하지 않는 옵션: $1 (--help 참고)" ;;
    esac
    if [[ "$1" == *=* ]]; then value="${1#*=}"; shift
    else [[ $# -ge 2 && "$2" != --* ]] || fail "$option 값이 필요합니다."; value="$2"; shift 2; fi
    [[ -n "$value" ]] || fail "$option 값이 비어 있습니다."
    case "$option" in
        --variant) [[ "$selected" == false ]] || fail 'variant는 한 번만 지정하세요.'; variant="$value"; selected=true ;;
        --output-dir) output="$value" ;;
        --intervals)
            [[ -z "$interval_selector" ]] || fail 'intervals는 한 번만 지정하세요.'
            [[ "$value" == all || "$value" =~ ^(1000|30000|60000|300000)(,(1000|30000|60000|300000))*$ ]] || fail 'intervals는 all 또는 1000/30000/60000/300000의 쉼표 목록입니다.'
            interval_selector="$value" ;;
        --scan-interval-ms|--inactivity-check-interval-ms)
            [[ "$value" =~ ^[1-9][0-9]{0,8}$ ]] || fail '조회·검사 간격은 양의 정수(ms)입니다.'
            manual_interval=true
            if [[ "$option" == --scan-interval-ms ]]; then scan_ms="$value"; else check_ms="$value"; fi ;;
        --deletion-mode)
            [[ -z "$deletion_filter" ]] || fail 'deletion-mode는 한 번만 지정하세요.'
            deletion_filter="${value^^}"
            [[ "$deletion_filter" == BATCH || "$deletion_filter" == IMMEDIATE ]] || fail '삭제 방식은 BATCH 또는 IMMEDIATE입니다.' ;;
        --hz) [[ "$selection_seen" == false ]] || fail 'Hz 선택은 한 번만 지정하세요.'; selection=(--hz "$value"); selection_seen=true ;;
        *) forwarded+=("$option" "$value") ;;
    esac
done
[[ -z "$interval_selector" || "$manual_interval" == false ]] || fail '--intervals와 개별 조회·검사 간격을 동시에 지정하지 마세요.'
intervals=("${interval_selector:-custom}")
if [[ "$interval_selector" == all ]]; then
    intervals=(1000 30000 60000 300000)
elif [[ -n "$interval_selector" ]]; then
    IFS=, read -r -a intervals <<< "$interval_selector"
fi
declare -A interval_seen=()
for interval in "${intervals[@]}"; do
    [[ -z "${interval_seen[$interval]:-}" ]] || fail "중복된 간격: $interval"
    interval_seen[$interval]=1
done
variants=(delete_off_fio_off delete_batch_fio_off delete_immediate_fio_off delete_off_fio_on delete_batch_fio_on delete_immediate_fio_on)
if [[ "$variant" == all && -n "$deletion_filter" ]]; then
    variants=(delete_off_fio_off "delete_${deletion_filter,,}_fio_off" delete_off_fio_on "delete_${deletion_filter,,}_fio_on")
fi
if [[ "$variant" != all ]]; then
    # 이전 delete_on 진입점의 기본 BATCH 및 명시적 IMMEDIATE 선택을 유지한다.
    case "$variant" in
        delete_on_fio_off|delete_on_fio_on)
            chosen="${deletion_filter:-BATCH}"
            variant="${variant/delete_on/delete_${chosen,,}}" ;;
    esac
    case "$variant" in
        delete_off_fio_off|delete_batch_fio_off|delete_immediate_fio_off|delete_off_fio_on|delete_batch_fio_on|delete_immediate_fio_on) variants=("$variant") ;;
        *) fail "잘못된 variant: $variant" ;;
    esac
    if [[ "$variant" != delete_off_* && -n "$deletion_filter" && "$variant" != delete_${deletion_filter,,}_* ]]; then
        fail 'variant에 명시된 삭제 방식과 --deletion-mode가 충돌합니다.'
    fi
fi
# 이 실행기를 통한 중복 실행을 막는다. 다른 진입점으로 시작한 실험은 별도 확인해야 한다.
command -v flock >/dev/null || fail 'flock이 필요합니다.'
command -v setsid >/dev/null || fail 'setsid가 필요합니다.'
exec 9>"$base/.client42_comparison.lock"
flock -n 9 || fail '다른 delete/fio 비교 실행이 진행 중입니다.'
mkdir -p -- "$output"
output="$(cd "$output" && pwd)"
run_dir="$(mktemp -d "$output/matrix_$(TZ=Asia/Seoul date +%Y%m%d_%H%M%S)_KST_XXXXXX")"
log() { printf '[%s] %s\n' "$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M:%S KST')" "$*" | tee -a "$run_dir/matrix.log"; }
job_pid=""
on_signal() {
    local status="$1"
    trap '' INT TERM HUP
    if [[ -n "$job_pid" ]]; then
        kill -TERM -- "-$job_pid" 2>/dev/null || true
        wait "$job_pid" || true
    fi
    exit "$status"
}
trap 'on_signal 130' INT
trap 'on_signal 143' TERM
trap 'on_signal 129' HUP
trap 'status=$?; log "비교 실행 종료: exit_code=$status, 결과=$run_dir"' EXIT
{ printf 'ORIGINAL_COMMAND='; printf '%q ' bash "$base/run_client42_comparison.sh" "${original[@]}"; printf '\n'; } > "$run_dir/matrix.run.txt"
printf 'variant\tscan_interval_ms\tinactivity_check_interval_ms\tstart_kst\tend_kst\texit_code\toutput_dir\n' > "$run_dir/matrix.tsv"
for interval in "${intervals[@]}"; do
    if [[ "$interval" != custom ]]; then scan_ms="$interval"; check_ms="$interval"; fi
    interval_label="scan_${scan_ms}ms_check_${check_ms}ms"
for variant in "${variants[@]}"; do
    switches=(--no-delete --no-fio)
    case "$variant" in
        delete_batch_fio_off) switches=(--delete --deletion-mode BATCH --no-fio) ;;
        delete_immediate_fio_off) switches=(--delete --deletion-mode IMMEDIATE --no-fio) ;;
        delete_off_fio_on) switches=(--no-delete --fio) ;;
        delete_batch_fio_on) switches=(--delete --deletion-mode BATCH --fio) ;;
        delete_immediate_fio_on) switches=(--delete --deletion-mode IMMEDIATE --fio) ;;
    esac
    variant_dir="$run_dir/$interval_label/$variant"
    mkdir -p "$variant_dir"
    command=(bash "$base/run_remote_kafka_experiments.sh"
        "${selection[@]}" --retry 3 --topic-mode auto
        --no-build --no-drop-caches --no-iostat --stats-enabled --cleanup
        --brokers 166.104.110.11:9092 --prefix test_creator
        --run-duration-minutes 20 --topic-cnt -1
        --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096
        --start-index 0 --start-barrier-delay-ms 10000
        --topic-prepare-timeout-seconds 600 --step-delay-seconds 10
        --inter-experiment-delay-seconds 60
        --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600
        --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3
        --reserve-gib 2 --expected-disk nvme1n1
        --deletion-mode BATCH --scan-interval-ms "$scan_ms"
        --inactivity-check-interval-ms "$check_ms" --inactivity-threshold-ms 600000
        --scan-log-interval-ms 60000
        "${forwarded[@]}" "${switches[@]}" --output-dir "$variant_dir")
    { printf '%s_%s_COMMAND=' "$interval_label" "$variant"; printf '%q ' "${command[@]}"; printf '\n'; } >> "$run_dir/matrix.run.txt"
    started="$(TZ=Asia/Seoul date -Is)"
    log "시작: $variant, 조회=${scan_ms}ms, 검사=${check_ms}ms, 결과=$variant_dir"
    setsid "${command[@]}" &
    job_pid=$!
    if wait "$job_pid"; then status=0; else status=$?; fi
    job_pid=""
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$variant" "$scan_ms" "$check_ms" "$started" "$(TZ=Asia/Seoul date -Is)" "$status" "$variant_dir" >> "$run_dir/matrix.tsv"
    log "완료: $variant, exit_code=$status"
    if (( status != 0 )); then
        log '하위 실행에서 실패/중단이 보고되어 다음 조합을 시작하지 않습니다. 회차별 기록을 확인하세요.'
        exit "$status"
    fi
done
done
