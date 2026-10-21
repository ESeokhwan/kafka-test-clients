# Kafka 실험 실행 블록: precreate·auto × .11·.42 × 1~5Hz

이 문서는 20개 케이스를 각각 독립 실행 블록으로 제공합니다. 원하는 케이스의 bash 블록 전체를 해당 서버의 터미널에 복사해서 실행하세요. 직전 요청에 맞춰 모든 케이스에 `--retry 3`을 적용했습니다.

## 공통 조건

| 항목 | 적용값 |
| --- | --- |
| 브로커 | 166.104.110.11:9092 |
| 토픽 접두어 | test_creator |
| 케이스별 반복 | 각 Hz 총 3회 (`--retry 3`) |
| 회차별 생산 시작 구간 | 10분 |
| 차량당 생산 | 100Hz, 메시지 본문 4096바이트 |
| 차량 생산 수명 | 40초 |
| 토픽 수 | `--topic-cnt -1`: Hz와 실험 시간으로 산정 |
| TRIM | ON, .11의 /data4, 예상 장치 nvme1n1 |
| TRIM 후 대기 | 매 회차 600초(10분), 최소 600초 |
| 단계 사이 대기 | 실제 실행 단계 사이 10초 |
| 종료 정리 구간 | cleanup과 대기를 합쳐 최소 60초, cleanup이 길면 완료까지 진행 |
| 종료 cleanup | ON |
| 실시간 비활성 토픽 삭제 | OFF (`--no-delete`) |
| fio / cache drop / build / iostat | 모두 OFF |
| 생산 통계 | ON (`--stats-enabled`) |
| 사전 토픽 준비·검사 제한 | 600초 |
| 생산 시작 barrier | 10초 |

- `--retry 3`은 실패 후 추가 재시도 3회가 아니라 **해당 Hz의 총 실행 횟수 3회**입니다.
- 이 문서의 각 블록은 단일 Hz만 실행합니다. 예를 들어 2Hz 블록은 2Hz를 3회 실행하고 종료합니다.
- 한 블록은 TRIM 후 대기 30분 + 생산 시작 구간 30분에 준비·마지막 차량 생산·정리 시간이 추가됩니다.
- 20개 블록을 모두 실행하면 총 60회입니다. TRIM 후 대기와 생산 시작 구간만 합쳐 20시간이며 추가 준비/정리 시간이 필요합니다.
- 두 서버와 두 모드 모두 같은 브로커·토픽 이름을 사용합니다. **이 문서의 블록을 동시에 실행하지 마세요.** 다른 작업에서도 `test_creator_0_*` 이름을 공유하지 마세요.
- `--no-delete`는 생산 중의 비활성 삭제기를 끕니다. `--cleanup`에 의한 회차 종료 삭제는 수행됩니다.
- `--no-build`이므로 실행 서버에 최신 JAR가 있어야 합니다. Java 소스를 바꾼 후에는 `--build`로 변경하세요.
- 전체 블록은 hss544 계정으로 실행합니다. TRIM에만 비대화식 sudo 권한이 필요하며 전체 스크립트 앞에 sudo를 붙이지 않습니다.
- TRIM은 지정 디렉터리만 정리하는 기능이 아니라 그 디렉터리가 속한 **파일시스템 전체의 빈 블록**에 적용됩니다. 대기가 SSD 내부 GC 완료를 보장하지는 않습니다.

## 실행 위치와 모드 차이

| 작업 | .11에서 실행하는 블록 | .42에서 실행하는 블록 |
| --- | --- | --- |
| 진입점 | run_kafka_experiments.sh | run_remote_kafka_experiments.sh |
| 프로젝트 경로 | /data4/kafka-test-clients | /home/hss544/kafka-test-clients |
| 토픽 생성·준비 메시지·데이터 생산 | .11 | .42 → .11 브로커 |
| cleanup 요청 | .11 | .42 → .11 브로커 |
| fstrim 실제 실행 | .11 | .42의 SSH 요청으로 .11에서 실행 |
| 결과 CSV·실행 설정·단계 로그 | .11의 output-dir | .42의 output-dir |
| build/cache drop/fio/iostat을 나중에 켠 경우 | .11 | .11에서 원격 실행 |

**auto 회차 순서:** 기존 토픽 충돌 검사 → TRIM → 600초 대기 → 생산 중 새 차량 토픽 생성·데이터 전송 → 종료 cleanup·정리 대기. 기존 토픽이 남으면 충돌 검사에서 중단하므로 임의로 계속 실행하지 않습니다. 브로커의 자동 토픽 생성 설정이 필요합니다.

**precreate 회차 순서:** 해당 회차 이름 범위의 기존 토픽 정리 → 충돌 검사 → TRIM → 600초 대기 → 전체 토픽 사전 생성·리더/ISR 확인·토픽당 준비 메시지 1개 전송 및 응답 확인 → 기존 토픽의 생산을 설정 Hz로 시작 → 종료 cleanup·정리 대기. 준비 메시지는 본 생산 CSV 통계에서 제외되지만 실제 디스크·오프셋에는 반영됩니다.

모든 블록에서 fio와 cache drop은 꺼져 있습니다. precreate에서 Hz는 사전 생성 속도가 아니라 차량별 생산 시작 빈도입니다.

| Hz | 회차당 계획 토픽 수(10분) | 정상상태 활성 차량/토픽 수(40초 수명, 이론값) |
| --- | ---: | ---: |
| 1 | 600 | 40 |
| 2 | 1,200 | 80 |
| 3 | 1,800 | 120 |
| 4 | 2,400 | 160 |
| 5 | 3,000 | 200 |

각 반복에서 토픽 이름을 재사용하고 cleanup합니다. 활성 토픽은 현재 데이터를 생산하는 토픽을 뜻하며, 생산 종료 후에도 cleanup 전까지 브로커에 남는 토픽과 구분합니다.

## 목차

- [A. .11 서버 / precreate](#a-11-precreate)
- [B. .11 서버 / auto](#b-11-auto)
- [C. .42 서버 / precreate](#c-42-precreate)
- [D. .42 서버 / auto](#d-42-auto)

<a id="a-11-precreate"></a>

## A. .11 서버 / precreate

실행 서버: **166.104.110.11**. 토픽 생성·생산과 브로커가 같은 .11 서버의 자원을 사용합니다.

### A-1. precreate / .11 / 1Hz / 총 3회

회차당 계획 토픽 600개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/data4/kafka-test-clients/experiment_results/client11_precreate_trim_retry3/1Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /data4/kafka-test-clients

  # 실행 위치: .11 / 브로커: .11 / precreate / 1Hz를 총 3회 반복
  # TRIM·대기 후 전체 토픽을 미리 만들고 토픽당 준비 메시지 1개를 전송
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_kafka_experiments.sh \
    --hz 1 --retry 3 --topic-mode precreate \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /data4/kafka-test-clients/experiment_results/client11_precreate_trim_retry3/1Hz
)
```

### A-2. precreate / .11 / 2Hz / 총 3회

회차당 계획 토픽 1200개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/data4/kafka-test-clients/experiment_results/client11_precreate_trim_retry3/2Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /data4/kafka-test-clients

  # 실행 위치: .11 / 브로커: .11 / precreate / 2Hz를 총 3회 반복
  # TRIM·대기 후 전체 토픽을 미리 만들고 토픽당 준비 메시지 1개를 전송
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_kafka_experiments.sh \
    --hz 2 --retry 3 --topic-mode precreate \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /data4/kafka-test-clients/experiment_results/client11_precreate_trim_retry3/2Hz
)
```

### A-3. precreate / .11 / 3Hz / 총 3회

회차당 계획 토픽 1800개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/data4/kafka-test-clients/experiment_results/client11_precreate_trim_retry3/3Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /data4/kafka-test-clients

  # 실행 위치: .11 / 브로커: .11 / precreate / 3Hz를 총 3회 반복
  # TRIM·대기 후 전체 토픽을 미리 만들고 토픽당 준비 메시지 1개를 전송
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_kafka_experiments.sh \
    --hz 3 --retry 3 --topic-mode precreate \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /data4/kafka-test-clients/experiment_results/client11_precreate_trim_retry3/3Hz
)
```

### A-4. precreate / .11 / 4Hz / 총 3회

회차당 계획 토픽 2400개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/data4/kafka-test-clients/experiment_results/client11_precreate_trim_retry3/4Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /data4/kafka-test-clients

  # 실행 위치: .11 / 브로커: .11 / precreate / 4Hz를 총 3회 반복
  # TRIM·대기 후 전체 토픽을 미리 만들고 토픽당 준비 메시지 1개를 전송
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_kafka_experiments.sh \
    --hz 4 --retry 3 --topic-mode precreate \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /data4/kafka-test-clients/experiment_results/client11_precreate_trim_retry3/4Hz
)
```

### A-5. precreate / .11 / 5Hz / 총 3회

회차당 계획 토픽 3000개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/data4/kafka-test-clients/experiment_results/client11_precreate_trim_retry3/5Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /data4/kafka-test-clients

  # 실행 위치: .11 / 브로커: .11 / precreate / 5Hz를 총 3회 반복
  # TRIM·대기 후 전체 토픽을 미리 만들고 토픽당 준비 메시지 1개를 전송
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_kafka_experiments.sh \
    --hz 5 --retry 3 --topic-mode precreate \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /data4/kafka-test-clients/experiment_results/client11_precreate_trim_retry3/5Hz
)
```

<a id="b-11-auto"></a>

## B. .11 서버 / auto

실행 서버: **166.104.110.11**. 토픽 생성·생산과 브로커가 같은 .11 서버의 자원을 사용합니다.

### B-1. auto / .11 / 1Hz / 총 3회

회차당 계획 토픽 600개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/data4/kafka-test-clients/experiment_results/client11_auto_trim_retry3/1Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /data4/kafka-test-clients

  # 실행 위치: .11 / 브로커: .11 / auto / 1Hz를 총 3회 반복
  # TRIM·대기 후 생산 중 차량 진입 빈도에 맞춰 새 토픽을 생성
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_kafka_experiments.sh \
    --hz 1 --retry 3 --topic-mode auto \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /data4/kafka-test-clients/experiment_results/client11_auto_trim_retry3/1Hz
)
```

### B-2. auto / .11 / 2Hz / 총 3회

회차당 계획 토픽 1200개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/data4/kafka-test-clients/experiment_results/client11_auto_trim_retry3/2Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /data4/kafka-test-clients

  # 실행 위치: .11 / 브로커: .11 / auto / 2Hz를 총 3회 반복
  # TRIM·대기 후 생산 중 차량 진입 빈도에 맞춰 새 토픽을 생성
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_kafka_experiments.sh \
    --hz 2 --retry 3 --topic-mode auto \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /data4/kafka-test-clients/experiment_results/client11_auto_trim_retry3/2Hz
)
```

### B-3. auto / .11 / 3Hz / 총 3회

회차당 계획 토픽 1800개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/data4/kafka-test-clients/experiment_results/client11_auto_trim_retry3/3Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /data4/kafka-test-clients

  # 실행 위치: .11 / 브로커: .11 / auto / 3Hz를 총 3회 반복
  # TRIM·대기 후 생산 중 차량 진입 빈도에 맞춰 새 토픽을 생성
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_kafka_experiments.sh \
    --hz 3 --retry 3 --topic-mode auto \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /data4/kafka-test-clients/experiment_results/client11_auto_trim_retry3/3Hz
)
```

### B-4. auto / .11 / 4Hz / 총 3회

회차당 계획 토픽 2400개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/data4/kafka-test-clients/experiment_results/client11_auto_trim_retry3/4Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /data4/kafka-test-clients

  # 실행 위치: .11 / 브로커: .11 / auto / 4Hz를 총 3회 반복
  # TRIM·대기 후 생산 중 차량 진입 빈도에 맞춰 새 토픽을 생성
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_kafka_experiments.sh \
    --hz 4 --retry 3 --topic-mode auto \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /data4/kafka-test-clients/experiment_results/client11_auto_trim_retry3/4Hz
)
```

### B-5. auto / .11 / 5Hz / 총 3회

회차당 계획 토픽 3000개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/data4/kafka-test-clients/experiment_results/client11_auto_trim_retry3/5Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /data4/kafka-test-clients

  # 실행 위치: .11 / 브로커: .11 / auto / 5Hz를 총 3회 반복
  # TRIM·대기 후 생산 중 차량 진입 빈도에 맞춰 새 토픽을 생성
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_kafka_experiments.sh \
    --hz 5 --retry 3 --topic-mode auto \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /data4/kafka-test-clients/experiment_results/client11_auto_trim_retry3/5Hz
)
```

<a id="c-42-precreate"></a>

## C. .42 서버 / precreate

실행 서버: **166.104.110.42**. 토픽 생성·생산·cleanup 요청은 .42에서 실행하며, TRIM은 SSH를 통해 .11에서 실행합니다. .42 → .11 비대화식 SSH 접속이 가능해야 합니다.

### C-1. precreate / .42 / 1Hz / 총 3회

회차당 계획 토픽 600개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/home/hss544/kafka-test-clients/experiment_results/client42_precreate_trim_retry3/1Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients

  # 실행 위치: .42 / 브로커: .11 / precreate / 1Hz를 총 3회 반복
  # TRIM·대기 후 전체 토픽을 미리 만들고 토픽당 준비 메시지 1개를 전송
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_remote_kafka_experiments.sh \
    --hz 1 --retry 3 --topic-mode precreate \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_precreate_trim_retry3/1Hz
)
```

### C-2. precreate / .42 / 2Hz / 총 3회

회차당 계획 토픽 1200개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/home/hss544/kafka-test-clients/experiment_results/client42_precreate_trim_retry3/2Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients

  # 실행 위치: .42 / 브로커: .11 / precreate / 2Hz를 총 3회 반복
  # TRIM·대기 후 전체 토픽을 미리 만들고 토픽당 준비 메시지 1개를 전송
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_remote_kafka_experiments.sh \
    --hz 2 --retry 3 --topic-mode precreate \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_precreate_trim_retry3/2Hz
)
```

### C-3. precreate / .42 / 3Hz / 총 3회

회차당 계획 토픽 1800개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/home/hss544/kafka-test-clients/experiment_results/client42_precreate_trim_retry3/3Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients

  # 실행 위치: .42 / 브로커: .11 / precreate / 3Hz를 총 3회 반복
  # TRIM·대기 후 전체 토픽을 미리 만들고 토픽당 준비 메시지 1개를 전송
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_remote_kafka_experiments.sh \
    --hz 3 --retry 3 --topic-mode precreate \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_precreate_trim_retry3/3Hz
)
```

### C-4. precreate / .42 / 4Hz / 총 3회

회차당 계획 토픽 2400개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/home/hss544/kafka-test-clients/experiment_results/client42_precreate_trim_retry3/4Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients

  # 실행 위치: .42 / 브로커: .11 / precreate / 4Hz를 총 3회 반복
  # TRIM·대기 후 전체 토픽을 미리 만들고 토픽당 준비 메시지 1개를 전송
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_remote_kafka_experiments.sh \
    --hz 4 --retry 3 --topic-mode precreate \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_precreate_trim_retry3/4Hz
)
```

### C-5. precreate / .42 / 5Hz / 총 3회

회차당 계획 토픽 3000개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/home/hss544/kafka-test-clients/experiment_results/client42_precreate_trim_retry3/5Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients

  # 실행 위치: .42 / 브로커: .11 / precreate / 5Hz를 총 3회 반복
  # TRIM·대기 후 전체 토픽을 미리 만들고 토픽당 준비 메시지 1개를 전송
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_remote_kafka_experiments.sh \
    --hz 5 --retry 3 --topic-mode precreate \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_precreate_trim_retry3/5Hz
)
```

<a id="d-42-auto"></a>

## D. .42 서버 / auto

실행 서버: **166.104.110.42**. 토픽 생성·생산·cleanup 요청은 .42에서 실행하며, TRIM은 SSH를 통해 .11에서 실행합니다. .42 → .11 비대화식 SSH 접속이 가능해야 합니다.

### D-1. auto / .42 / 1Hz / 총 3회

회차당 계획 토픽 600개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/home/hss544/kafka-test-clients/experiment_results/client42_auto_trim_retry3/1Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients

  # 실행 위치: .42 / 브로커: .11 / auto / 1Hz를 총 3회 반복
  # TRIM·대기 후 생산 중 차량 진입 빈도에 맞춰 새 토픽을 생성
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_remote_kafka_experiments.sh \
    --hz 1 --retry 3 --topic-mode auto \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_auto_trim_retry3/1Hz
)
```

### D-2. auto / .42 / 2Hz / 총 3회

회차당 계획 토픽 1200개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/home/hss544/kafka-test-clients/experiment_results/client42_auto_trim_retry3/2Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients

  # 실행 위치: .42 / 브로커: .11 / auto / 2Hz를 총 3회 반복
  # TRIM·대기 후 생산 중 차량 진입 빈도에 맞춰 새 토픽을 생성
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_remote_kafka_experiments.sh \
    --hz 2 --retry 3 --topic-mode auto \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_auto_trim_retry3/2Hz
)
```

### D-3. auto / .42 / 3Hz / 총 3회

회차당 계획 토픽 1800개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/home/hss544/kafka-test-clients/experiment_results/client42_auto_trim_retry3/3Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients

  # 실행 위치: .42 / 브로커: .11 / auto / 3Hz를 총 3회 반복
  # TRIM·대기 후 생산 중 차량 진입 빈도에 맞춰 새 토픽을 생성
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_remote_kafka_experiments.sh \
    --hz 3 --retry 3 --topic-mode auto \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_auto_trim_retry3/3Hz
)
```

### D-4. auto / .42 / 4Hz / 총 3회

회차당 계획 토픽 2400개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/home/hss544/kafka-test-clients/experiment_results/client42_auto_trim_retry3/4Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients

  # 실행 위치: .42 / 브로커: .11 / auto / 4Hz를 총 3회 반복
  # TRIM·대기 후 생산 중 차량 진입 빈도에 맞춰 새 토픽을 생성
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_remote_kafka_experiments.sh \
    --hz 4 --retry 3 --topic-mode auto \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_auto_trim_retry3/4Hz
)
```

### D-5. auto / .42 / 5Hz / 총 3회

회차당 계획 토픽 3000개, 각 차량 생산 수명 40초, 생산 시작 구간 10분입니다. 결과 상위 폴더: `/home/hss544/kafka-test-clients/experiment_results/client42_auto_trim_retry3/5Hz`. 반복별 하위 폴더와 CSV·로그는 자동 분리됩니다.

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients

  # 실행 위치: .42 / 브로커: .11 / auto / 5Hz를 총 3회 반복
  # TRIM·대기 후 생산 중 차량 진입 빈도에 맞춰 새 토픽을 생성
  # 매 회차 TRIM 후 600초 대기 / 생산 시작 구간 10분 / 종료 cleanup ON
  # build·cache drop·iostat·fio·실시간 delete OFF
  bash ./run_remote_kafka_experiments.sh \
    --hz 5 --retry 3 --topic-mode auto \
    --no-build --no-drop-caches --no-iostat \
    --no-fio --no-delete --cleanup --stats-enabled \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 10 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 --inter-experiment-delay-seconds 60 \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --expected-disk nvme1n1 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_auto_trim_retry3/5Hz
)
```

## 옵션을 변경하는 방법

아래 값은 실행 블록 안에서 교체하세요. `--hz`와 `--all`은 중복 지정하지 않습니다.

| 바꾸려는 조건 | 변경 |
| --- | --- |
| 각 Hz를 한 번만 실행 | `--retry 3` → `--retry 1` |
| 회차 생산 시작 구간을 20분으로 설정 | `--run-duration-minutes 10` → `--run-duration-minutes 20` |
| TRIM 후 20분 대기 | `--fstrim-wait-seconds 600` → `--fstrim-wait-seconds 1200` |
| TRIM과 대기 생략 | `--fstrim` → `--no-fstrim` |
| 실험 전후 cache drop 활성화 | `--no-drop-caches` → `--drop-caches` |
| 1초 iostat 로그 활성화 | `--no-iostat` → `--iostat` |
| 최신 소스로 JAR 빌드 | `--no-build` → `--build`; .42 블록에서는 .11 빌드 후 JAR를 .42로 동기화 |
| 1~5Hz를 한 명령으로 순차 실행 | `--hz N` → `--all`; output-dir의 /NHz도 /all_hz로 변경 |

`--all --retry 3`의 순서는 1Hz×3 → 2Hz×3 → 3Hz×3 → 4Hz×3 → 5Hz×3입니다. 위 20개 개별 블록과 이 통합 실행을 중복 수행하지 않도록 선택하세요.

fio를 켜려면 `--no-fio`를 `--fio`로 교체하고, `--fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 --reserve-gib 2`를 지정하세요. 이 경로는 .42 실행에서도 .11 기준입니다. fio는 TRIM·대기 이후에 수행하며 precreate에서는 사전 토픽 생성 이후에 수행합니다.

## 결과 추적과 실패 처리

- output-dir 아래에 Hz·반복·토픽 모드·fio/delete·시간 조건·회차 시작 KST가 포함된 하위 폴더가 생성됩니다.
- 회차 로그와 CSV 파일명은 회차 시작 시각을 포함하고, 종료 처리 시 종료 KST도 반영합니다.
- `.run.txt`에 원본 실행 명령·적용 설정·실행 호스트·단계 이벤트를 기록합니다. `_runs.tsv`로 회차별 산출물을 연결합니다.
- `_fstrim.log`에 TRIM 실행 결과를 보관하고, TRIM 후 대기 진행은 1초마다 KST로 터미널과 실행 기록에 남깁니다.
- .42 분산 실행의 .11 작업 로그는 `/data4/kafka-test-clients/.remote_jobs/`에도 보관됩니다.
- 일반적인 회차 실패는 로그를 남기고 cleanup 후 다음 반복으로 진행합니다. 시작 전 안전 검사 실패, cleanup 복구 실패, 사용자 중단 등은 전체 실행을 중단할 수 있습니다. 일부 회차가 실패했다면 전체 종료 코드도 확인하세요.
- 이 문서는 실행용 명령을 제공합니다. 문서 작성·배포 검증은 명령 문법 검사만 수행하며 실제 실험을 시작하지 않습니다.
