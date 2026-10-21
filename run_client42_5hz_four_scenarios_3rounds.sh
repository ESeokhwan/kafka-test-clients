#!/usr/bin/env bash
# .42에서 실행: PRECREATE/OFF → AUTO/OFF → AUTO/IMMEDIATE 1초 → AUTO/IMMEDIATE 300초.
# 위 네 조건을 모두 실행한 뒤 다음 회차로 넘어가며, 기본 총 3회차(12개 실험)다.
# 각 실험은 60분, fio OFF, .11에서 fstrim 후 600초 대기, iostat 1초 수집이다.
# JAR는 사전에 배포된 동일 빌드를 사용한다. drop caches는 OFF다.
# 실패 시 cleanup 완료가 확인된 경우에만 60초 후 1회 재시도한다.
# 사용: bash ./run_client42_5hz_four_scenarios_3rounds.sh
# 계획 확인: bash ./run_client42_5hz_four_scenarios_3rounds.sh --dry-run
# 뒤에 전달한 지원 옵션은 기본값을 덮어쓴다. 예: --rounds 2
set -Eeuo pipefail
base="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$base/run_client42_suite.sh" \
  --scenario-set precreate-auto \
  --hz-list 5 --rounds 3 \
  --delete-modes IMMEDIATE --intervals 1000 300000 \
  --run-duration-minutes 60 --fio-passes 0 \
  --fstrim --fstrim-target-dir /data4 --expected-disk nvme1n1 \
  --fstrim-wait-seconds 600 --topic-prepare-timeout-seconds 600 \
  --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
  --cleanup-timeout-seconds 600 --require-cleanup-success \
  --failure-retries 1 --retry-delay-seconds 60 --iostat \
  --output-dir "$base/experiment_results" "$@"
