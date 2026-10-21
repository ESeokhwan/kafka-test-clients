# Kafka 실험 명령 — 개별 Hz 실행

모든 블록은 `--build`(기본 ON), `--retry 1`, `--topic-mode auto`, `--step-delay-seconds 10`, `--drop-caches`를 명시합니다. 최신 JAR를 재사용하려면 `--no-build`, 기본 ON인 실험 전후 파일 캐시 비우기를 끄려면 `--no-drop-caches`로 해당 플래그를 교체하세요. 캐시 비우기는 실행 서버 전체에 적용되며 sudo/root 권한이 필요합니다. `--retry N`은 해당 Hz를 총 N회 반복합니다. [precreate 명령](KAFKA_PRECREATED_SCENARIOS.md)은 `--all`을 `--hz 1`~`--hz 5`로 바꾸어 단독 실행할 수 있습니다.

이 문서는 **블록 하나당 지정된 Hz 하나만 실행**합니다. 조건 31개 × Hz 5개 = 총 155개의 복사 가능한 블록입니다. 필요한 블록만 선택하여 실행하세요. 모든 조합을 순차 실행하려면 [순차 실행 문서](KAFKA_EXPERIMENTS_SEQUENTIAL.md)를 사용하세요.

## 공통 조건

- 서버 위치: /data4/kafka-test-clients/
- 삭제 OFF: 20분. 삭제 ON: 20/40/60분 × BATCH/IMMEDIATE × 조회 1초/30초/1분/5분/10분.
- 비활성 기준: 600000 ms (10분). 조회 주기와 비활성 검사 주기를 동일하게 지정합니다.
- 매 실행 전 fio 준비 ON, 동일 토픽 이름 재사용을 위한 즉시 정리는 필수입니다.
- 단일 Hz에도 cleanup 및 정리·대기 시간을 적용합니다. cleanup 시간이 60초 미만이면 남은 시간만 기다리고, 더 오래 걸리면 완료까지 기다립니다.
- 종료 후 공통 접두어 test_creator_0_ 아래 해당 회차의 인덱스 범위만 정리합니다. 시작 전에 같은 이름의 토픽이 있으면 중단합니다.
- 브로커 166.104.110.11:9092 / JMX 127.0.0.1:9999 / 장치 nvme1n1이 의도한 서버 설정인지 확인하세요.
- 결과 디렉터리는 조건/Hz별로 분리합니다. 동일 조건의 접두어 범위에서는 이전 실행 토픽도 시간 기반 삭제 대상이 될 수 있습니다.
- 데몬 타이머는 재시작 시 초기화되며 최초 오프셋 관측부터 시간을 측정합니다. 조회가 10분인 20분 실험에서는 삭제가 적거나 없을 수 있습니다.


## 실행 인자와 CSV 추적

최신 run_kafka_experiments.sh를 서버에 반영하면 실험 시작 전에 다음 파일을 생성합니다.

- `<tag>_metrics.csv`: 생산 결과
- `<tag>_deletion.csv`: 삭제 결과 (삭제 ON일 때)
- `<tag>.run.txt`: 원본 명령, 환경 변수/기본값/CLI를 반영한 적용 설정, 실제 생산/fio/삭제 명령, 실험 접두어, CSV 절대 경로, UTC 단계 시각 및 종료 상태
- `<실행ID>_runs.tsv`: 같은 실행에서 생성한 Hz별 기록과 CSV 경로의 연결표

예를 들어 `abc_0_3Hz_metrics.csv`의 실행 조건은 `abc_0_3Hz.run.txt`에서 찾습니다. TXT의 HZ, RUN_DURATION_MINUTES, DELETE_ENABLED, DELETION_MODE, SCAN_INTERVAL_MS, INACTIVITY_THRESHOLD_MS 및 PRODUCER_COMMAND 등을 확인하세요. 실제 명령은 Bash %q 형식으로 인수 경계를 보존합니다. sha256sum이 있으면 실행 셸, fio, Java 소스, 생산 실행기의 해시도 저장합니다. JAR 해시와 빌드/재사용 여부도 기록하지만 브로커 설정 전체의 재현을 보장하는 기록은 아닙니다.

실패한 경우에도 이미 시작한 실험의 TXT에 종료 코드가 남습니다. SIGKILL·전원 장애처럼 트랩이 실행되지 않으면 STARTED 또는 마지막 단계만 남을 수 있습니다. 경로가 기록되었다고 CSV 생성까지 성공했다는 뜻은 아니므로 status=SUCCESS 및 실제 파일 존재를 함께 확인하세요. CSV 자체의 컬럼은 변경하지 않습니다.


## 개별 실행 명령

### 01. 삭제 OFF · 20분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 OFF / 각 Hz 20분 / 조회·삭제 데몬은 실행하지 않음
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --no-delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 1 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_off_20m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 OFF / 각 Hz 20분 / 조회·삭제 데몬은 실행하지 않음
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --no-delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 1 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_off_20m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 OFF / 각 Hz 20분 / 조회·삭제 데몬은 실행하지 않음
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --no-delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 1 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_off_20m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 OFF / 각 Hz 20분 / 조회·삭제 데몬은 실행하지 않음
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --no-delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_off_20m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 OFF / 각 Hz 20분 / 조회·삭제 데몬은 실행하지 않음
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --no-delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_off_20m/5Hz
)
```

### 02. 삭제 ON · 20분 · BATCH · 조회 1초

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 1초
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_1s/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 1초
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_1s/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 1초
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_1s/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 1초
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_1s/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 1초
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_1s/5Hz
)
```

### 03. 삭제 ON · 20분 · BATCH · 조회 30초

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 30초
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_30s/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 30초
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_30s/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 30초
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_30s/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 30초
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_30s/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 30초
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_30s/5Hz
)
```

### 04. 삭제 ON · 20분 · BATCH · 조회 1분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 1분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_1m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 1분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_1m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 1분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_1m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 1분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_1m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 1분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_1m/5Hz
)
```

### 05. 삭제 ON · 20분 · BATCH · 조회 5분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 5분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_5m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 5분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_5m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 5분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_5m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 5분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_5m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 5분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_5m/5Hz
)
```

### 06. 삭제 ON · 20분 · BATCH · 조회 10분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 10분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_10m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 10분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_10m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 10분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_10m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 10분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_10m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 10분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_10m/5Hz
)
```

### 07. 삭제 ON · 20분 · IMMEDIATE · 조회 1초

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 1초
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_1s/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 1초
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_1s/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 1초
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_1s/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 1초
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_1s/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 1초
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_1s/5Hz
)
```

### 08. 삭제 ON · 20분 · IMMEDIATE · 조회 30초

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 30초
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_30s/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 30초
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_30s/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 30초
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_30s/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 30초
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_30s/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 30초
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_30s/5Hz
)
```

### 09. 삭제 ON · 20분 · IMMEDIATE · 조회 1분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 1분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_1m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 1분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_1m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 1분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_1m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 1분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_1m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 1분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_1m/5Hz
)
```

### 10. 삭제 ON · 20분 · IMMEDIATE · 조회 5분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 5분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_5m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 5분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_5m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 5분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_5m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 5분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_5m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 5분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_5m/5Hz
)
```

### 11. 삭제 ON · 20분 · IMMEDIATE · 조회 10분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 10분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_10m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 10분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_10m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 10분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_10m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 10분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_10m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 10분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_10m/5Hz
)
```

### 12. 삭제 ON · 40분 · BATCH · 조회 1초

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 1초
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_1s/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 1초
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_1s/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 1초
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_1s/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 1초
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_1s/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 1초
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_1s/5Hz
)
```

### 13. 삭제 ON · 40분 · BATCH · 조회 30초

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 30초
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_30s/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 30초
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_30s/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 30초
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_30s/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 30초
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_30s/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 30초
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_30s/5Hz
)
```

### 14. 삭제 ON · 40분 · BATCH · 조회 1분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 1분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_1m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 1분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_1m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 1분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_1m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 1분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_1m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 1분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_1m/5Hz
)
```

### 15. 삭제 ON · 40분 · BATCH · 조회 5분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 5분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_5m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 5분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_5m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 5분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_5m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 5분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_5m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 5분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_5m/5Hz
)
```

### 16. 삭제 ON · 40분 · BATCH · 조회 10분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 10분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_10m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 10분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_10m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 10분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_10m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 10분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_10m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 10분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_10m/5Hz
)
```

### 17. 삭제 ON · 40분 · IMMEDIATE · 조회 1초

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 1초
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_1s/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 1초
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_1s/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 1초
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_1s/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 1초
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_1s/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 1초
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_1s/5Hz
)
```

### 18. 삭제 ON · 40분 · IMMEDIATE · 조회 30초

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 30초
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_30s/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 30초
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_30s/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 30초
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_30s/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 30초
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_30s/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 30초
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_30s/5Hz
)
```

### 19. 삭제 ON · 40분 · IMMEDIATE · 조회 1분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 1분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_1m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 1분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_1m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 1분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_1m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 1분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_1m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 1분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_1m/5Hz
)
```

### 20. 삭제 ON · 40분 · IMMEDIATE · 조회 5분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 5분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_5m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 5분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_5m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 5분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_5m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 5분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_5m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 5분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_5m/5Hz
)
```

### 21. 삭제 ON · 40분 · IMMEDIATE · 조회 10분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 10분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_10m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 10분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_10m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 10분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_10m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 10분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_10m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 10분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 40 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_10m/5Hz
)
```

### 22. 삭제 ON · 60분 · BATCH · 조회 1초

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 1초
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_1s/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 1초
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_1s/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 1초
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_1s/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 1초
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_1s/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 1초
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_1s/5Hz
)
```

### 23. 삭제 ON · 60분 · BATCH · 조회 30초

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 30초
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_30s/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 30초
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_30s/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 30초
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_30s/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 30초
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_30s/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 30초
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_30s/5Hz
)
```

### 24. 삭제 ON · 60분 · BATCH · 조회 1분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 1분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_1m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 1분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_1m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 1분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_1m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 1분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_1m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 1분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_1m/5Hz
)
```

### 25. 삭제 ON · 60분 · BATCH · 조회 5분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 5분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_5m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 5분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_5m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 5분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_5m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 5분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_5m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 5분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_5m/5Hz
)
```

### 26. 삭제 ON · 60분 · BATCH · 조회 10분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 10분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_10m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 10분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_10m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 10분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_10m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 10분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_10m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 10분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode BATCH \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_10m/5Hz
)
```

### 27. 삭제 ON · 60분 · IMMEDIATE · 조회 1초

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 1초
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_1s/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 1초
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_1s/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 1초
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_1s/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 1초
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_1s/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 1초
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_1s/5Hz
)
```

### 28. 삭제 ON · 60분 · IMMEDIATE · 조회 30초

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 30초
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_30s/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 30초
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_30s/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 30초
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_30s/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 30초
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_30s/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 30초
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 30000 --inactivity-check-interval-ms 30000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_30s/5Hz
)
```

### 29. 삭제 ON · 60분 · IMMEDIATE · 조회 1분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 1분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_1m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 1분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_1m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 1분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_1m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 1분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_1m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 1분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 60000 --inactivity-check-interval-ms 60000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_1m/5Hz
)
```

### 30. 삭제 ON · 60분 · IMMEDIATE · 조회 5분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 5분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_5m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 5분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_5m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 5분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_5m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 5분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_5m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 5분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 300000 --inactivity-check-interval-ms 300000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_5m/5Hz
)
```

### 31. 삭제 ON · 60분 · IMMEDIATE · 조회 10분

#### 1 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 10분
  # 1 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 1 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_10m/1Hz
)
```

#### 2 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 10분
  # 2 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 2 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_10m/2Hz
)
```

#### 3 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 10분
  # 3 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 3 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_10m/3Hz
)
```

#### 4 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 10분
  # 4 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 4 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_10m/4Hz
)
```

#### 5 Hz 단독 실행

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 10분
  # 5 Hz만 한 번 실행. 실행 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --hz 5 --fio --iostat --stats-enabled \
    --delete --cleanup \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator --delete-topic-prefix test_creator_ \
    --run-duration-minutes 60 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --inactivity-threshold-ms 600000 \
    --deletion-mode IMMEDIATE \
    --scan-interval-ms 600000 --inactivity-check-interval-ms 600000 \
    --scan-log-interval-ms 60000 \
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 \
    --reserve-gib 2 --expected-disk nvme1n1 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_10m/5Hz
)
```

## iostat 1초 디스크 상태 로그와 정리 시간

- iostat은 기본 켜짐이며 `--iostat` / `--no-iostat`로 제어합니다. 서버에 sysstat 패키지의 iostat이 필요합니다.
- `iostat -d -x -t -y 1`로 전체 디스크의 확장 I/O 통계를 1초 간격으로 기록합니다. 부팅 이후 누적 첫 보고서는 생략합니다. fio 직전부터 생산 및 정리·대기 종료까지 `<tag>_iostat.log`에 저장합니다.
- `.run.txt`의 IOSTAT_COMMAND, IOSTAT_LOG와 `_runs.tsv`의 iostat_log 열에서 CSV와 연결할 수 있습니다. 단계별 UTC 기록의 FIO, PRODUCER, CLEANUP, COOLDOWN_START/END로 구간을 구분합니다.
- 예: 딜레이 60초, cleanup 15초이면 남은 45초만 대기합니다. cleanup 80초이면 80초 후 진행합니다. 딜레이 0이면 추가 대기는 없지만 cleanup 완료까지 기다립니다. 단독 `--mode cleanup`은 요청된 즉시 정리만 수행하며 실험 딜레이는 적용하지 않습니다.
- 실시간으로 보려면 별도 터미널에서 `tail -f /실제/결과경로/실제태그_iostat.log`를 사용하세요.
- iostat 옵션 설명: [Linux iostat 매뉴얼](https://www.man7.org/linux/man-pages/man1/iostat.1.html).
