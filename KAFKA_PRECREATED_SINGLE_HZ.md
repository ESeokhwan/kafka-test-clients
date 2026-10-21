# 사전 토픽 생성 실험 — 개별 Hz 실행

각 코드 블록은 그 Hz만 1회 실행합니다. [1~5 Hz 순차 실행 문서](KAFKA_PRECREATED_SCENARIOS.md)와 동일한 조건이며 fio ON 5개, OFF 5개입니다. 원하는 블록 하나 전체를 복사해 터미널에 붙여넣으세요.

## 공통 설정과 주의사항

- 모든 명령은 서버 `166.104.110.11`의 `/data4/kafka-test-clients`에서 **hss544 계정**으로 실행합니다. 전체 스크립트에 sudo를 붙이지 마세요. root 소유 빌드 산출물이 생겨 다음 빌드가 실패할 수 있습니다.
- 각 블록은 단독 복사·실행할 수 있습니다. **동시에 실행하지 마세요.** 모든 블록이 `test_creator_0_인덱스` 토픽을 재사용하며 시작 전·종료 후 cleanup으로 해당 회차 범위의 토픽과 데이터를 삭제합니다. 다른 작업과 이 이름 범위를 공유하지 마세요.
- 기본 예제는 `--build`, `--retry 1`, `--step-delay-seconds 10`, `--drop-caches`입니다. 빌드는 명령 호출당 한 번이며, `--retry`는 실패 재시도 횟수가 아니라 **각 Hz의 총 실험 횟수**입니다.
- 검증된 최신 JAR를 재사용하려면 `--build`를 `--no-build`로 바꿉니다. Java 소스 변경 후에는 빌드가 필요합니다.
- 캐시 비우기는 기본 ON입니다. 끄려면 `--drop-caches`를 `--no-drop-caches`로 바꿉니다. 회차 준비 전과 종료 cleanup·대기 후 실행하며, 필요한 작업에만 내부적으로 `sudo -n`을 사용합니다. 필요하면 같은 터미널에서 먼저 sudo 인증을 하고 권한 유효 시간을 확인하세요.
- `--no-delete`는 생산 중 비활성 토픽 삭제기를 끄는 옵션입니다. 시작 전·종료 후 `--cleanup`은 계속 수행합니다. precreate에서는 `--delete`를 사용할 수 없습니다.
- 20분은 차량/생산 작업자를 시작시키는 설정 구간입니다. 준비·fio·마지막 차량 생산·cleanup·대기가 더해져 실제 실행 시간은 더 깁니다.

준비 단계에서 각 토픽에 메시지 1개를 전송하고 ACK를 확인합니다. 이 메시지는 본 생산 CSV 통계에서는 제외되지만 토픽에는 남습니다. 세부 동작·토픽 수·로그 해석은 [순차 실행 문서](KAFKA_PRECREATED_SCENARIOS.md)를 참고하세요.

## A. fio ON

### 1 Hz · fio ON

사전 생성 1200개 · 준비 메시지 1200개 · 안정 상태 활성 생산 토픽 목표 40개.

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # hss544 계정으로 실행: 전체 스크립트 앞에 sudo를 붙이지 않습니다.
  # 1 Hz만 실행 / 각 Hz 1회 / 생산 시작 구간 20분 / fio ON
  # 토픽 생성 후 각 토픽에 준비 메시지 1개를 보내고 ACK를 확인합니다.
  bash ./run_kafka_experiments.sh \
    --build --step-delay-seconds 10 --drop-caches \
    --hz 1 --retry 1 --topic-mode precreate \
    --fio --no-delete --cleanup --iostat --stats-enabled \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/precreated_fio/1Hz
)
```

### 2 Hz · fio ON

사전 생성 2400개 · 준비 메시지 2400개 · 안정 상태 활성 생산 토픽 목표 80개.

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # hss544 계정으로 실행: 전체 스크립트 앞에 sudo를 붙이지 않습니다.
  # 2 Hz만 실행 / 각 Hz 1회 / 생산 시작 구간 20분 / fio ON
  # 토픽 생성 후 각 토픽에 준비 메시지 1개를 보내고 ACK를 확인합니다.
  bash ./run_kafka_experiments.sh \
    --build --step-delay-seconds 10 --drop-caches \
    --hz 2 --retry 1 --topic-mode precreate \
    --fio --no-delete --cleanup --iostat --stats-enabled \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/precreated_fio/2Hz
)
```

### 3 Hz · fio ON

사전 생성 3600개 · 준비 메시지 3600개 · 안정 상태 활성 생산 토픽 목표 120개.

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # hss544 계정으로 실행: 전체 스크립트 앞에 sudo를 붙이지 않습니다.
  # 3 Hz만 실행 / 각 Hz 1회 / 생산 시작 구간 20분 / fio ON
  # 토픽 생성 후 각 토픽에 준비 메시지 1개를 보내고 ACK를 확인합니다.
  bash ./run_kafka_experiments.sh \
    --build --step-delay-seconds 10 --drop-caches \
    --hz 3 --retry 1 --topic-mode precreate \
    --fio --no-delete --cleanup --iostat --stats-enabled \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/precreated_fio/3Hz
)
```

### 4 Hz · fio ON

사전 생성 4800개 · 준비 메시지 4800개 · 안정 상태 활성 생산 토픽 목표 160개.

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # hss544 계정으로 실행: 전체 스크립트 앞에 sudo를 붙이지 않습니다.
  # 4 Hz만 실행 / 각 Hz 1회 / 생산 시작 구간 20분 / fio ON
  # 토픽 생성 후 각 토픽에 준비 메시지 1개를 보내고 ACK를 확인합니다.
  bash ./run_kafka_experiments.sh \
    --build --step-delay-seconds 10 --drop-caches \
    --hz 4 --retry 1 --topic-mode precreate \
    --fio --no-delete --cleanup --iostat --stats-enabled \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/precreated_fio/4Hz
)
```

### 5 Hz · fio ON

사전 생성 6000개 · 준비 메시지 6000개 · 안정 상태 활성 생산 토픽 목표 200개.

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # hss544 계정으로 실행: 전체 스크립트 앞에 sudo를 붙이지 않습니다.
  # 5 Hz만 실행 / 각 Hz 1회 / 생산 시작 구간 20분 / fio ON
  # 토픽 생성 후 각 토픽에 준비 메시지 1개를 보내고 ACK를 확인합니다.
  bash ./run_kafka_experiments.sh \
    --build --step-delay-seconds 10 --drop-caches \
    --hz 5 --retry 1 --topic-mode precreate \
    --fio --no-delete --cleanup --iostat --stats-enabled \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/precreated_fio/5Hz
)
```

## B. fio OFF

### 1 Hz · fio OFF

사전 생성 1200개 · 준비 메시지 1200개 · 안정 상태 활성 생산 토픽 목표 40개.

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # hss544 계정으로 실행: 전체 스크립트 앞에 sudo를 붙이지 않습니다.
  # 1 Hz만 실행 / 각 Hz 1회 / 생산 시작 구간 20분 / fio OFF
  # 토픽 생성 후 각 토픽에 준비 메시지 1개를 보내고 ACK를 확인합니다.
  bash ./run_kafka_experiments.sh \
    --build --step-delay-seconds 10 --drop-caches \
    --hz 1 --retry 1 --topic-mode precreate \
    --no-fio --no-delete --cleanup --iostat --stats-enabled \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/precreated_no_fio/1Hz
)
```

### 2 Hz · fio OFF

사전 생성 2400개 · 준비 메시지 2400개 · 안정 상태 활성 생산 토픽 목표 80개.

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # hss544 계정으로 실행: 전체 스크립트 앞에 sudo를 붙이지 않습니다.
  # 2 Hz만 실행 / 각 Hz 1회 / 생산 시작 구간 20분 / fio OFF
  # 토픽 생성 후 각 토픽에 준비 메시지 1개를 보내고 ACK를 확인합니다.
  bash ./run_kafka_experiments.sh \
    --build --step-delay-seconds 10 --drop-caches \
    --hz 2 --retry 1 --topic-mode precreate \
    --no-fio --no-delete --cleanup --iostat --stats-enabled \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/precreated_no_fio/2Hz
)
```

### 3 Hz · fio OFF

사전 생성 3600개 · 준비 메시지 3600개 · 안정 상태 활성 생산 토픽 목표 120개.

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # hss544 계정으로 실행: 전체 스크립트 앞에 sudo를 붙이지 않습니다.
  # 3 Hz만 실행 / 각 Hz 1회 / 생산 시작 구간 20분 / fio OFF
  # 토픽 생성 후 각 토픽에 준비 메시지 1개를 보내고 ACK를 확인합니다.
  bash ./run_kafka_experiments.sh \
    --build --step-delay-seconds 10 --drop-caches \
    --hz 3 --retry 1 --topic-mode precreate \
    --no-fio --no-delete --cleanup --iostat --stats-enabled \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/precreated_no_fio/3Hz
)
```

### 4 Hz · fio OFF

사전 생성 4800개 · 준비 메시지 4800개 · 안정 상태 활성 생산 토픽 목표 160개.

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # hss544 계정으로 실행: 전체 스크립트 앞에 sudo를 붙이지 않습니다.
  # 4 Hz만 실행 / 각 Hz 1회 / 생산 시작 구간 20분 / fio OFF
  # 토픽 생성 후 각 토픽에 준비 메시지 1개를 보내고 ACK를 확인합니다.
  bash ./run_kafka_experiments.sh \
    --build --step-delay-seconds 10 --drop-caches \
    --hz 4 --retry 1 --topic-mode precreate \
    --no-fio --no-delete --cleanup --iostat --stats-enabled \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/precreated_no_fio/4Hz
)
```

### 5 Hz · fio OFF

사전 생성 6000개 · 준비 메시지 6000개 · 안정 상태 활성 생산 토픽 목표 200개.

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # hss544 계정으로 실행: 전체 스크립트 앞에 sudo를 붙이지 않습니다.
  # 5 Hz만 실행 / 각 Hz 1회 / 생산 시작 구간 20분 / fio OFF
  # 토픽 생성 후 각 토픽에 준비 메시지 1개를 보내고 ACK를 확인합니다.
  bash ./run_kafka_experiments.sh \
    --build --step-delay-seconds 10 --drop-caches \
    --hz 5 --retry 1 --topic-mode precreate \
    --no-fio --no-delete --cleanup --iostat --stats-enabled \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/precreated_no_fio/5Hz
)
```

