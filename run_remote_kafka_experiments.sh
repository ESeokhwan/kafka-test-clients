#!/usr/bin/env bash
# .42용 진입점. 서버 자원 작업은 .11, Kafka 클라이언트 작업은 현재 호스트에서 실행한다.
set -Eeuo pipefail
base="$(cd "$(dirname "$0")" && pwd)"
exec bash "$base/run_kafka_experiments.sh" \
  --broker-ssh hss544@166.104.110.11 \
  --broker-project-dir /data4/kafka-test-clients \
  --jmx-host 166.104.110.11 "$@"
