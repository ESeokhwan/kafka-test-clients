#!/usr/bin/env bash
# 배포/빌드 후 .42에서 수동 실행하는 대조 실험. 이 파일 작성만으로 실험은 실행되지 않는다.
set -Eeuo pipefail
base="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
dry_run=false
if [[ "${1:-}" == --dry-run ]]; then dry_run=true; shift; fi
hz="${1:-4}"
rounds="${2:-3}"
minutes="${3:-60}"
[[ "$hz" =~ ^[1-5]$ ]] || { echo 'Hz: 1~5 정수'; exit 2; }
[[ "$rounds" =~ ^[1-9][0-9]*$ && "$minutes" =~ ^[1-9][0-9]*$ ]] || {
  echo '반복/시간은 양의 정수'; exit 2;
}
root="$base/experiment_results/timing_$(TZ=Asia/Seoul date +%Y%m%d_%H%M%S)_KST"
if [[ "$dry_run" == false ]]; then
  # 오래된 JAR로 fstrim/토픽 준비부터 수행하지 않도록 진단 옵션 지원을 먼저 확인한다.
  if ! help="$(java -cp "$base/build/libs/apps-1.0-SNAPSHOT-all.jar" org.example.apps.AutoTopicProducerMetrics --help)"; then
    echo 'JAR 도움말 확인 실패. 새 소스로 빌드하고 .42에 배포하세요.'; exit 2;
  fi
  [[ "$help" == *--timing-diagnostics* && "$help" == *--send-phase-jitter-ms* ]] || {
    echo '먼저 새 소스로 JAR를 빌드하고 .42에 배포하세요.'; exit 2;
  }
  mkdir -p "$root"
fi
# 측정 자체의 영향도 확인한다: off=기존, on=진단만, jitter=진단+송신 시점 분산.
# AUTO에도 같은 진단/분산을 적용하여 비교 조건을 맞춘다.
variants=(pre_off auto_off pre_on auto_on pre_jitter auto_jitter)
for ((round=1; round<=rounds; round++)); do
  for ((slot=0; slot<${#variants[@]}; slot++)); do
    # 홀수/짝수 회차의 실행 순서를 반대로 하여 고정 순서 영향을 줄인다.
    index=$slot
    (( round % 2 == 1 )) || index=$((${#variants[@]}-1-slot))
    variant="${variants[index]}"
    mode=auto
    [[ "$variant" != pre_* ]] || mode=precreate
    diagnostic=()
    [[ "$variant" == *_off ]] || diagnostic+=(--timing-diagnostics)
    jitter=0
    [[ "$variant" != *_jitter ]] || jitter=10
    # seed는 회차별로 기록되며, 같은 회차 PRE/AUTO에는 같은 값을 적용한다.
    label="r${round}_${variant}"
    printf '[%s] %s: Hz=%s minutes=%s\n' \
      "$(TZ=Asia/Seoul date '+%F %T KST')" "$label" "$hz" "$minutes"
    # 실패 시 중단한다. cleanup 실패 상태에서 다음 조건을 자동 시작하지 않는다.
    command_args=(bash "$base/run_remote_kafka_experiments.sh" \
      --hz "$hz" --retry 1 --topic-mode "$mode" \
      --no-build --no-fio --no-delete --cleanup --no-drop-caches --iostat --stats-enabled \
      --brokers 166.104.110.11:9092 --prefix timing_probe \
      --run-duration-minutes "$minutes" --topic-cnt -1 \
      --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
      --start-index 0 --start-barrier-delay-ms 10000 \
      --topic-prepare-timeout-seconds 600 --cleanup-timeout-seconds 600 \
      --fstrim --fstrim-target-dir /data4 --expected-disk nvme1n1 --fstrim-wait-seconds 600 \
      --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
      --experiment-label "$label" --output-dir "$root" \
      -- "${diagnostic[@]}" --send-phase-jitter-ms "$jitter" --send-phase-seed "$round")
    if [[ "$dry_run" == true ]]; then
      printf '%q ' "${command_args[@]}"
      printf '\n'
    else
      "${command_args[@]}"
    fi
  done
done
