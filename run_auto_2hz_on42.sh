#!/usr/bin/env bash
# .42 서버에서 실행: 2Hz, auto, 생산 시작 구간 10분.
# TRIM은 .11의 /data4에서 실행하고 완료 후 최소 10분 기다린다.
# 실시간 삭제는 OFF, 종료 cleanup은 ON. 다른 실험과 동시에 실행하지 않는다.
set -Eeuo pipefail
cd /home/hss544/kafka-test-clients
# 뒤에 붙인 옵션으로 대기시간·토픽 모드 등 값을 변경할 수 있다.
exec bash ./run_remote_kafka_experiments.sh \
  --no-build --no-drop-caches --no-iostat \
  --hz 2 --retry 1 --topic-mode auto \
  --no-fio --no-delete --cleanup --stats-enabled \
  --brokers 166.104.110.11:9092 --prefix test_creator \
  --run-duration-minutes 10 --topic-cnt -1 \
  --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
  --start-index 0 --start-barrier-delay-ms 10000 \
  --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
  --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
  --expected-disk nvme1n1 \
  --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_trim/2Hz \
  "$@"
