# Kafka 실험 명령 — 1~5 Hz 순차 실행

모든 블록은 `--build`(기본 ON), `--retry 1`, `--topic-mode auto`, `--step-delay-seconds 10`, `--drop-caches`를 명시합니다. 최신 JAR를 그대로 쓰려면 `--build`를 `--no-build`로 바꾸세요. 파일 캐시 비우기는 기본 ON이며 `--drop-caches`를 `--no-drop-caches`로 바꾸면 비활성화됩니다(실행 서버 전체 적용, sudo/root 권한 필요). `--retry`는 실패 재시도 횟수가 아닌 각 Hz의 총 반복 횟수입니다. 생산 중 삭제가 없는 사전 생성 실험은 [precreate 명령](KAFKA_PRECREATED_SCENARIOS.md)을 사용하세요.

## 1. 실험 조건과 개수

이 문서의 bash 코드 블록은 전체를 복사하여 서버 터미널에 붙여넣을 수 있습니다. 실제 실험을 실행하는 명령이며, 동시에 실행하지 말고 순차 실행하세요.

| 구분 | 각 Hz 실행 시간 | 삭제 방식 | 조회 간격 | 조합 수 | Hz별 실험 수 |
|---|---|---|---|---:|---:|
| 삭제 OFF | 20분 | 해당 없음 | 해당 없음 | 1 | 5 |
| 삭제 ON | 20 / 40 / 60분 | BATCH / IMMEDIATE | 1초 / 30초 / 1분 / 5분 / 10분 | 30 | 150 |
| 합계 | | | | **31** | **155** |

- 모든 명령은 `--all`로 1→2→3→4→5 Hz를 한 번에 순차 실행합니다.
- 삭제 OFF에서는 삭제 방식과 조회 간격이 실제로 적용되지 않으므로 같은 실험을 중복하지 않습니다.
- 비활성 기준은 모두 `--inactivity-threshold-ms 600000` (10분)입니다.
- 조회 간격은 `--scan-interval-ms`와 `--inactivity-check-interval-ms`에 같은 값을 넣습니다. 이 구현은 비활성 검사 시 오프셋을 읽으므로 두 값을 함께 설정합니다.
- 로그 출력 간격은 60초로 고정합니다. 실제 로그 빈도는 조회 주기보다 빨라지지 않습니다.
- fio 준비는 매 Hz마다 수행하며, fio 진행률은 터미널에 표시하고 같은 내용은 *_fio.log에도 저장합니다. 예약 공간 2 GiB를 제외한 가용 영역을 총 3회 쓰고 파일을 지웁니다.
- `--cleanup`을 명시하여 매 실험 종료 후 해당 실험의 토픽을 정리합니다. 삭제 ON/OFF는 생산 중 시간 기반 삭제 여부이며, OFF에서도 생산 종료 후 cleanup은 수행합니다.
- 모든 조건의 토픽 접두어는 test_creator로 통일하며 동일 토픽 이름을 cleanup 후 재사용합니다. 결과 디렉터리만 조건별로 구분합니다. 동시에 여러 실험을 실행하지 않습니다.
- 매 Hz 종료 후 정리·대기 구간은 최소 60초입니다. cleanup에 걸린 시간을 포함하며, 마지막 Hz도 동일하게 적용합니다. 별도 sleep을 더하지 않습니다.

설정된 생산 시간의 합계는 **6,100분 = 101시간 40분**입니다. fio 155회, 시작 배리어, 마지막 작업자 종료 및 딜레이가 추가되므로 실제 총 소요 시간은 더 깁니다. `--run-duration-minutes`는 정확한 프로세스 종료 제한이 아니라 생성할 토픽 수 계산에 사용됩니다.

## 2. 실행 전 확인 및 해석

다음 파일을 /data4/kafka-test-clients/에 배치해야 합니다.

- run_kafka_experiments.sh
- prepare_ssd_fio.sh
- KafkaJmxProducerBatchDaemon.java
- scripts/kafka_clients_run.sh 및 빌드된 생산기 JAR

아래 명령은 기존 브로커 주소 **166.104.110.11:9092**, JMX **127.0.0.1:9999**, fio 장치 **nvme1n1**을 사용합니다. JMX가 해당 Kafka 클러스터의 의도한 브로커이고 /data4가 해당 SSD에 위치하는지 확인하세요. 주소가 다르면 명령의 --brokers / --jmx-host / --expected-disk를 변경하세요.

**cleanup은 이번 실험 접두어의 토픽을 삭제합니다.** 다른 실행의 잔여 토픽과 디스크 상태까지 초기화하지는 않습니다. 접두어는 모든 조건에서 동일하며 물리 디스크를 격리하지 않습니다. auto 모드에서는 시작 전 같은 이름의 토픽이 있으면 삭제 없이 중단합니다.

삭제 타이머는 데몬의 최초 오프셋 관측에서 시작하며 재시작 시 초기화됩니다. 조회 간격이 10분이면 삭제 판단도 10분 단위로 지연될 수 있습니다. 20분 실험에서는 처음 관측·마지막 증가 관측 시점에 따라 실험 중 삭제가 적거나 없을 수 있으며, 이는 설정의 결과입니다. 데몬은 각 Hz 생산 종료 시 함께 종료합니다.


## 실행 인자와 CSV 추적

최신 run_kafka_experiments.sh를 서버에 반영하면 실험 시작 전에 다음 파일을 생성합니다.

- `<tag>_metrics.csv`: 생산 결과
- `<tag>_deletion.csv`: 삭제 결과 (삭제 ON일 때)
- `<tag>.run.txt`: 원본 명령, 환경 변수/기본값/CLI를 반영한 적용 설정, 실제 생산/fio/삭제 명령, 실험 접두어, CSV 절대 경로, UTC 단계 시각 및 종료 상태
- `<실행ID>_runs.tsv`: 같은 실행에서 생성한 Hz별 기록과 CSV 경로의 연결표

예를 들어 `abc_0_3Hz_metrics.csv`의 실행 조건은 `abc_0_3Hz.run.txt`에서 찾습니다. TXT의 HZ, RUN_DURATION_MINUTES, DELETE_ENABLED, DELETION_MODE, SCAN_INTERVAL_MS, INACTIVITY_THRESHOLD_MS 및 PRODUCER_COMMAND 등을 확인하세요. 실제 명령은 Bash %q 형식으로 인수 경계를 보존합니다. sha256sum이 있으면 실행 셸, fio, Java 소스, 생산 실행기의 해시도 저장합니다. JAR 해시와 빌드/재사용 여부도 기록하지만 브로커 설정 전체의 재현을 보장하는 기록은 아닙니다.

실패한 경우에도 이미 시작한 실험의 TXT에 종료 코드가 남습니다. SIGKILL·전원 장애처럼 트랩이 실행되지 않으면 STARTED 또는 마지막 단계만 남을 수 있습니다. 경로가 기록되었다고 CSV 생성까지 성공했다는 뜻은 아니므로 status=SUCCESS 및 실제 파일 존재를 함께 확인하세요. CSV 자체의 컬럼은 변경하지 않습니다.

## 3. 전체 31개 조합을 한 번에 실행

아래 블록 하나만 실행하면 OFF 20분 → ON 20분 → ON 40분 → ON 60분 순서로 모두 실행합니다. 각 ON 시간에서 BATCH → IMMEDIATE 순서, 각 방식에서 1초 → 30초 → 1분 → 5분 → 10분 순서입니다.

**이 블록을 실행했다면 4절의 개별 블록을 다시 실행할 필요가 없습니다.** 실패하면 이후 조합을 중단하며 자동 재개는 하지 않습니다.

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 전체 명령에 공통으로 적용하는 생산/fio/시간 설정.
  common=(
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches
    --all --fio --iostat --stats-enabled --cleanup
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999
    --topic-cnt -1 --topic-lifetime-ms 40000
    --produce-hz-per-topic 100 --msg-size 4096
    --start-index 0 --start-barrier-delay-ms 10000
    --inactivity-threshold-ms 600000 --scan-log-interval-ms 60000
    --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3
    --reserve-gib 2 --expected-disk nvme1n1
    --inter-experiment-delay-seconds 60
  )

  # 1) 삭제 OFF: 각 Hz 20분씩 실행. 삭제 방식/조회 간격은 적용되지 않음.
  bash ./run_kafka_experiments.sh "${common[@]}" \
    --no-delete --run-duration-minutes 20 \
    --prefix test_creator \
    --delete-topic-prefix test_creator_ \
    --output-dir /data4/kafka-test-clients/experiment_results/delete_off_20m

  # 2) 삭제 ON: 3개 시간 × 2개 삭제 방식 × 5개 조회 간격.
  intervals=(1000 30000 60000 300000 600000)
  labels=(1s 30s 1m 5m 10m)
  for minutes in 20 40 60; do
    for deletion_mode in BATCH IMMEDIATE; do
      for index in "${!intervals[@]}"; do
        # 이전 조합의 마지막 실험에 정리·대기 시간이 이미 포함됩니다.
        interval="${intervals[index]}"
        case_id="delete_on_${minutes}m_${deletion_mode,,}_scan_${labels[index]}"
        echo "실행 조건: $case_id (1~5 Hz)"
        bash ./run_kafka_experiments.sh "${common[@]}" \
          --delete --run-duration-minutes "$minutes" \
          --deletion-mode "$deletion_mode" \
          --scan-interval-ms "$interval" \
          --inactivity-check-interval-ms "$interval" \
          --prefix test_creator \
          --delete-topic-prefix test_creator_ \
          --output-dir "/data4/kafka-test-clients/experiment_results/${case_id}"
      done
    done
  done
  echo "31개 조합 / 155개 Hz별 실험 완료"
)
```

## 4. 조건별 개별 실행 블록

원하는 조건의 블록 하나를 복사하면 그 조건에서 1~5 Hz가 모두 실행됩니다. 특정 Hz만 필요하면 해당 블록의 `--all`을 `--hz 3`처럼 변경하세요.

### 01. 삭제 OFF · 20분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_off_20m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 OFF / 각 Hz 20분 / 조회·삭제 데몬은 실행하지 않음
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_off_20m
)
```

### 02. 삭제 ON · 20분 · BATCH · 조회 1초

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_1s/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 1초
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_1s
)
```

### 03. 삭제 ON · 20분 · BATCH · 조회 30초

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_30s/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 30초
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_30s
)
```

### 04. 삭제 ON · 20분 · BATCH · 조회 1분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_1m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 1분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_1m
)
```

### 05. 삭제 ON · 20분 · BATCH · 조회 5분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_5m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 5분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_5m
)
```

### 06. 삭제 ON · 20분 · BATCH · 조회 10분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_10m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / BATCH / 조회·비활성 검사 10분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_batch_scan_10m
)
```

### 07. 삭제 ON · 20분 · IMMEDIATE · 조회 1초

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_1s/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 1초
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_1s
)
```

### 08. 삭제 ON · 20분 · IMMEDIATE · 조회 30초

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_30s/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 30초
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_30s
)
```

### 09. 삭제 ON · 20분 · IMMEDIATE · 조회 1분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_1m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 1분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_1m
)
```

### 10. 삭제 ON · 20분 · IMMEDIATE · 조회 5분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_5m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 5분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_5m
)
```

### 11. 삭제 ON · 20분 · IMMEDIATE · 조회 10분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_10m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 20분 / IMMEDIATE / 조회·비활성 검사 10분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_20m_immediate_scan_10m
)
```

### 12. 삭제 ON · 40분 · BATCH · 조회 1초

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_1s/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 1초
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_1s
)
```

### 13. 삭제 ON · 40분 · BATCH · 조회 30초

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_30s/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 30초
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_30s
)
```

### 14. 삭제 ON · 40분 · BATCH · 조회 1분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_1m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 1분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_1m
)
```

### 15. 삭제 ON · 40분 · BATCH · 조회 5분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_5m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 5분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_5m
)
```

### 16. 삭제 ON · 40분 · BATCH · 조회 10분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_10m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / BATCH / 조회·비활성 검사 10분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_batch_scan_10m
)
```

### 17. 삭제 ON · 40분 · IMMEDIATE · 조회 1초

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_1s/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 1초
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_1s
)
```

### 18. 삭제 ON · 40분 · IMMEDIATE · 조회 30초

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_30s/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 30초
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_30s
)
```

### 19. 삭제 ON · 40분 · IMMEDIATE · 조회 1분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_1m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 1분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_1m
)
```

### 20. 삭제 ON · 40분 · IMMEDIATE · 조회 5분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_5m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 5분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_5m
)
```

### 21. 삭제 ON · 40분 · IMMEDIATE · 조회 10분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_10m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 40분 / IMMEDIATE / 조회·비활성 검사 10분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_40m_immediate_scan_10m
)
```

### 22. 삭제 ON · 60분 · BATCH · 조회 1초

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_1s/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 1초
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_1s
)
```

### 23. 삭제 ON · 60분 · BATCH · 조회 30초

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_30s/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 30초
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_30s
)
```

### 24. 삭제 ON · 60분 · BATCH · 조회 1분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_1m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 1분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_1m
)
```

### 25. 삭제 ON · 60분 · BATCH · 조회 5분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_5m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 5분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_5m
)
```

### 26. 삭제 ON · 60분 · BATCH · 조회 10분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_10m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / BATCH / 조회·비활성 검사 10분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_batch_scan_10m
)
```

### 27. 삭제 ON · 60분 · IMMEDIATE · 조회 1초

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_1s/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 1초
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_1s
)
```

### 28. 삭제 ON · 60분 · IMMEDIATE · 조회 30초

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_30s/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 30초
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_30s
)
```

### 29. 삭제 ON · 60분 · IMMEDIATE · 조회 1분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_1m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 1분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_1m
)
```

### 30. 삭제 ON · 60분 · IMMEDIATE · 조회 5분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_5m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 5분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_5m
)
```

### 31. 삭제 ON · 60분 · IMMEDIATE · 조회 10분

결과 폴더: `/data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_10m/`

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 삭제 ON / 각 Hz 60분 / IMMEDIATE / 조회·비활성 검사 10분
  # --all: 1 → 2 → 3 → 4 → 5 Hz 순차 실행. 각 Hz 전에 fio 준비.
  bash ./run_kafka_experiments.sh \
    --build --retry 1 --topic-mode auto --step-delay-seconds 10 --drop-caches \
    --all --fio --iostat --stats-enabled \
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
    --output-dir /data4/kafka-test-clients/experiment_results/delete_on_60m_immediate_scan_10m
)
```

## 5. 각 Hz 실험의 실행 단계

1. 선택된 Hz와 결과 경로를 구성합니다.
2. fio 준비 쓰기 및 임시 파일 삭제를 완료합니다.
3. 삭제 ON이면 시간 기반 삭제기를 시작하고 준비 완료를 기다립니다. OFF이면 건너뜁니다.
4. 생산기를 실행합니다. ON에서는 오프셋 증가가 없는 시간이 10분을 넘으면 지정 주기에 검사하여 BATCH/IMMEDIATE로 삭제합니다.
5. 생산이 끝나면 삭제기를 종료하고 결과 CSV를 확인합니다.
6. 이번 실험의 토픽을 정리하고, 정리 시작부터 60초가 되도록 남은 시간만 기다립니다. 삭제에 60초 이상 걸리면 완료까지 기다립니다.
7. iostat 기록을 종료하고 다음 Hz를 진행합니다. 마지막 5 Hz도 정리·대기 후 종료하며 전체 블록은 다음 조합으로 넘어갑니다.

기존 기본값/환경 변수 중 명령에 명시하지 않은 경로·제한 시간 설정은 실행 환경에 따릅니다. 별도로 실행 중인 삭제 데몬이 있으면 OFF 조건에도 영향을 줄 수 있으므로 중복 실행 상태를 확인하세요.

## iostat 1초 디스크 상태 로그와 정리 시간

- iostat은 기본 켜짐이며 `--iostat` / `--no-iostat`로 제어합니다. 서버에 sysstat 패키지의 iostat이 필요합니다.
- `iostat -d -x -t -y 1`로 전체 디스크의 확장 I/O 통계를 1초 간격으로 기록합니다. 부팅 이후 누적 첫 보고서는 생략합니다. fio 직전부터 생산 및 정리·대기 종료까지 `<tag>_iostat.log`에 저장합니다.
- `.run.txt`의 IOSTAT_COMMAND, IOSTAT_LOG와 `_runs.tsv`의 iostat_log 열에서 CSV와 연결할 수 있습니다. 단계별 UTC 기록의 FIO, PRODUCER, CLEANUP, COOLDOWN_START/END로 구간을 구분합니다.
- 예: 딜레이 60초, cleanup 15초이면 남은 45초만 대기합니다. cleanup 80초이면 80초 후 진행합니다. 딜레이 0이면 추가 대기는 없지만 cleanup 완료까지 기다립니다. 단독 `--mode cleanup`은 요청된 즉시 정리만 수행하며 실험 딜레이는 적용하지 않습니다.
- 실시간으로 보려면 별도 터미널에서 `tail -f /실제/결과경로/실제태그_iostat.log`를 사용하세요.
- iostat 옵션 설명: [Linux iostat 매뉴얼](https://www.man7.org/linux/man-pages/man1/iostat.1.html).
