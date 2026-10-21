# Hz별 실행 블록 — .11 로컬 / .42 분산

각 블록은 하나의 독립 실험입니다. 두 서버 모두 같은 .11 브로커와 `test_creator` 토픽 이름을 사용하므로 블록을 동시에 실행하지 마세요.

공통 조건: auto 모드, 각 Hz 총 1회, 생산 시작 구간 10분, 차량 수명 40초, 차량당 100Hz, 메시지 4096바이트. build/cache drop/iostat/fio/실시간 delete는 OFF, cleanup과 fstrim은 ON입니다. TRIM 완료 후 600초 대기하며 준비·마지막 차량 생산·cleanup 때문에 전체 실행 시간은 20분보다 깁니다. `--no-delete`여도 종료 cleanup은 실행합니다.

.11은 모든 작업을 로컬에서 실행합니다. .42는 토픽 생성·생산·cleanup 요청을 실행하고 fstrim을 .11에 원격 요청합니다. 결과는 실행한 서버의 experiment_results 아래에 기록됩니다. 각 스크립트 뒤에 `--fstrim-wait-seconds 1200`을 붙이면 20분 대기, `--topic-mode precreate`를 붙이면 사전 생성으로 변경할 수 있습니다. Hz는 파일별 고정이므로 추가 `--hz`를 붙이지 마세요.

## .11 서버

### 1Hz

파일 실행: `bash /data4/kafka-test-clients/run_auto_1hz_on11.sh`

터미널에 바로 붙여넣기:

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 1Hz / 10분 / fstrim 후 600초 대기 / cleanup ON
  bash ./run_kafka_experiments.sh \
    --no-build --no-drop-caches --no-iostat \
    --hz 1 --retry 1 --topic-mode auto \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /data4/kafka-test-clients/experiment_results/client11_trim/1Hz
)
```

### 2Hz

파일 실행: `bash /data4/kafka-test-clients/run_auto_2hz_on11.sh`

터미널에 바로 붙여넣기:

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 2Hz / 10분 / fstrim 후 600초 대기 / cleanup ON
  bash ./run_kafka_experiments.sh \
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
    --output-dir /data4/kafka-test-clients/experiment_results/client11_trim/2Hz
)
```

### 3Hz

파일 실행: `bash /data4/kafka-test-clients/run_auto_3hz_on11.sh`

터미널에 바로 붙여넣기:

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 3Hz / 10분 / fstrim 후 600초 대기 / cleanup ON
  bash ./run_kafka_experiments.sh \
    --no-build --no-drop-caches --no-iostat \
    --hz 3 --retry 1 --topic-mode auto \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /data4/kafka-test-clients/experiment_results/client11_trim/3Hz
)
```

### 4Hz

파일 실행: `bash /data4/kafka-test-clients/run_auto_4hz_on11.sh`

터미널에 바로 붙여넣기:

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 4Hz / 10분 / fstrim 후 600초 대기 / cleanup ON
  bash ./run_kafka_experiments.sh \
    --no-build --no-drop-caches --no-iostat \
    --hz 4 --retry 1 --topic-mode auto \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /data4/kafka-test-clients/experiment_results/client11_trim/4Hz
)
```

### 5Hz

파일 실행: `bash /data4/kafka-test-clients/run_auto_5hz_on11.sh`

터미널에 바로 붙여넣기:

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # 5Hz / 10분 / fstrim 후 600초 대기 / cleanup ON
  bash ./run_kafka_experiments.sh \
    --no-build --no-drop-caches --no-iostat \
    --hz 5 --retry 1 --topic-mode auto \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /data4/kafka-test-clients/experiment_results/client11_trim/5Hz
)
```

## .42 서버

### 1Hz

파일 실행: `bash /home/hss544/kafka-test-clients/run_auto_1hz_on42.sh`

터미널에 바로 붙여넣기:

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 1Hz / 10분 / fstrim 후 600초 대기 / cleanup ON
  bash ./run_remote_kafka_experiments.sh \
    --no-build --no-drop-caches --no-iostat \
    --hz 1 --retry 1 --topic-mode auto \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_trim/1Hz
)
```

### 2Hz

파일 실행: `bash /home/hss544/kafka-test-clients/run_auto_2hz_on42.sh`

터미널에 바로 붙여넣기:

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 2Hz / 10분 / fstrim 후 600초 대기 / cleanup ON
  bash ./run_remote_kafka_experiments.sh \
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
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_trim/2Hz
)
```

### 3Hz

파일 실행: `bash /home/hss544/kafka-test-clients/run_auto_3hz_on42.sh`

터미널에 바로 붙여넣기:

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 3Hz / 10분 / fstrim 후 600초 대기 / cleanup ON
  bash ./run_remote_kafka_experiments.sh \
    --no-build --no-drop-caches --no-iostat \
    --hz 3 --retry 1 --topic-mode auto \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_trim/3Hz
)
```

### 4Hz

파일 실행: `bash /home/hss544/kafka-test-clients/run_auto_4hz_on42.sh`

터미널에 바로 붙여넣기:

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 4Hz / 10분 / fstrim 후 600초 대기 / cleanup ON
  bash ./run_remote_kafka_experiments.sh \
    --no-build --no-drop-caches --no-iostat \
    --hz 4 --retry 1 --topic-mode auto \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_trim/4Hz
)
```

### 5Hz

파일 실행: `bash /home/hss544/kafka-test-clients/run_auto_5hz_on42.sh`

터미널에 바로 붙여넣기:

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 5Hz / 10분 / fstrim 후 600초 대기 / cleanup ON
  bash ./run_remote_kafka_experiments.sh \
    --no-build --no-drop-caches --no-iostat \
    --hz 5 --retry 1 --topic-mode auto \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_trim/5Hz
)
```
