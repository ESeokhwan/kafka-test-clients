# 사전 토픽 생성 실험 — 순차 실행

개별 Hz 실행은 [precreate 개별 Hz 블록](KAFKA_PRECREATED_SINGLE_HZ.md)을 사용하세요. 이 문서의 `--all` 블록은 1~5 Hz를 순서대로 실행합니다.

## 공통 설정과 주의사항

- 모든 명령은 서버 `166.104.110.11`의 `/data4/kafka-test-clients`에서 **hss544 계정**으로 실행합니다. 전체 스크립트에 sudo를 붙이지 마세요. root 소유 빌드 산출물이 생겨 다음 빌드가 실패할 수 있습니다.
- 각 블록은 단독 복사·실행할 수 있습니다. **동시에 실행하지 마세요.** 모든 블록이 `test_creator_0_인덱스` 토픽을 재사용하며 시작 전·종료 후 cleanup으로 해당 회차 범위의 토픽과 데이터를 삭제합니다. 다른 작업과 이 이름 범위를 공유하지 마세요.
- 기본 예제는 `--build`, `--retry 1`, `--step-delay-seconds 10`, `--drop-caches`입니다. 빌드는 명령 호출당 한 번이며, `--retry`는 실패 재시도 횟수가 아니라 **각 Hz의 총 실험 횟수**입니다.
- 검증된 최신 JAR를 재사용하려면 `--build`를 `--no-build`로 바꿉니다. Java 소스 변경 후에는 빌드가 필요합니다.
- 캐시 비우기는 기본 ON입니다. 끄려면 `--drop-caches`를 `--no-drop-caches`로 바꿉니다. 회차 준비 전과 종료 cleanup·대기 후 실행하며, 필요한 작업에만 내부적으로 `sudo -n`을 사용합니다. 필요하면 같은 터미널에서 먼저 sudo 인증을 하고 권한 유효 시간을 확인하세요.
- `--no-delete`는 생산 중 비활성 토픽 삭제기를 끄는 옵션입니다. 시작 전·종료 후 `--cleanup`은 계속 수행합니다. precreate에서는 `--delete`를 사용할 수 없습니다.
- 20분은 차량/생산 작업자를 시작시키는 설정 구간입니다. 준비·fio·마지막 차량 생산·cleanup·대기가 더해져 실제 실행 시간은 더 깁니다.

## 시나리오 A: 사전 생성·준비 메시지 → fio → 본 생산 → 정리

1~5 Hz, 각 1회, fio ON, 캐시 비우기 ON입니다.

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # hss544 계정으로 실행: 전체 스크립트 앞에 sudo를 붙이지 않습니다.
  # 1 → 2 → 3 → 4 → 5 Hz 순차 실행 / 각 Hz 1회 / 생산 시작 구간 20분 / fio ON
  # 토픽 생성 후 각 토픽에 준비 메시지 1개를 보내고 ACK를 확인합니다.
  bash ./run_kafka_experiments.sh \
    --build --step-delay-seconds 10 --drop-caches \
    --all --retry 3 --topic-mode precreate \
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
    --output-dir /data4/kafka-test-clients/experiment_results/precreated_fio
)
```

## 시나리오 B: 사전 생성·준비 메시지 → 본 생산 → 정리

1~5 Hz, 각 1회, fio OFF, 캐시 비우기 ON입니다.

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # hss544 계정으로 실행: 전체 스크립트 앞에 sudo를 붙이지 않습니다.
  # 1 → 2 → 3 → 4 → 5 Hz 순차 실행 / 각 Hz 1회 / 생산 시작 구간 20분 / fio OFF
  # 토픽 생성 후 각 토픽에 준비 메시지 1개를 보내고 ACK를 확인합니다.
  bash ./run_kafka_experiments.sh \
    --build --step-delay-seconds 60 --drop-caches \
    --all --retry 3 --topic-mode precreate \
    --no-fio --no-delete --cleanup --iostat --stats-enabled \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/precreated_no_fio
)
```

## 추가 예제: fio ON · 캐시 비우기 ON · 각 Hz 3회

기존 3회 반복 조합입니다. 1 Hz를 3회 완료한 뒤 2 Hz로 넘어갑니다. 총 15회이며 각 회차 전후 캐시 비우기를 수행합니다. 사용 전 내부 `sudo -n` 실행 권한을 준비하세요.

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # hss544 계정으로 실행: 전체 스크립트 앞에 sudo를 붙이지 않습니다.
  # 1 → 2 → 3 → 4 → 5 Hz 순차 실행 / 각 Hz 3회 / 생산 시작 구간 20분 / fio ON
  # 토픽 생성 후 각 토픽에 준비 메시지 1개를 보내고 ACK를 확인합니다.
  bash ./run_kafka_experiments.sh \
    --build --step-delay-seconds 10 --drop-caches \
    --all --retry 3 --topic-mode precreate \
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
    --output-dir /data4/kafka-test-clients/experiment_results/precreated_fio_repeat3_drop_caches
)
```

## 비교용: auto 모드 · fio OFF

토픽 사전 생성·준비 메시지 없이, 차량별 첫 전송으로 토픽을 생성하는 비교군입니다. 기존 토픽이 있으면 보호를 위해 시작 전 검사가 중단됩니다.

```bash
(
  set -e
  cd /data4/kafka-test-clients
  # hss544 계정으로 실행: 전체 스크립트 앞에 sudo를 붙이지 않습니다.
  # 1 → 2 → 3 → 4 → 5 Hz 순차 실행 / 각 Hz 1회 / 생산 시작 구간 20분 / fio OFF
  # 사전 생성 없이 각 차량의 첫 메시지 전송으로 토픽을 생성합니다.
  bash ./run_kafka_experiments.sh \
    --build --step-delay-seconds 10 --drop-caches \
    --all --retry 1 --topic-mode auto \
    --no-fio --no-delete --cleanup --iostat --stats-enabled \
    --brokers 166.104.110.11:9092 --jmx-host 127.0.0.1 --jmx-port 9999 \
    --prefix test_creator \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /data4/kafka-test-clients/experiment_results/auto_no_fio
)
```

## 현재 precreate 동작과 결과 해석

1. 선택적 캐시 비우기 → 해당 회차 토픽 범위 시작 전 cleanup → 토픽 부재 확인.
2. 전체 토픽 사전 생성 → 리더/ISR 확인 → **각 토픽에 `--msg-size` 크기의 준비 메시지 1개 전송·`acks=all` 응답 확인**.
3. fio ON이면 남은 가용 공간을 기준으로 준비 작업을 수행하고 임시 파일을 삭제합니다. OFF이면 건너뜁니다.
4. 토픽 존재·리더/ISR 재확인 → 시작 배리어 → 설정 Hz로 차량의 생산 작업자를 시작합니다. 재확인 때 준비 메시지를 다시 보내지 않습니다.
5. 결과 확인 → 종료 cleanup 및 최소 정리·대기 시간 → 선택적 캐시 비우기 → 종료 시각으로 결과 파일명 확정.

준비 메시지 전송은 precreate에 자동 포함되므로 별도 옵션이 필요하지 않습니다. 본 생산 CSV 통계에서는 제외되지만 실제 토픽에는 남아 디스크 사용량과 오프셋에 반영됩니다. `_precreate.log`에 ACK 완료 개수를, `.run.txt`에 `PRECREATE_SEED_*` 설정을 기록합니다. 생성과 준비 전송은 합쳐서 `--topic-prepare-timeout-seconds 600` 제한을 사용하고 생산 전 재확인에는 별도 제한이 적용됩니다. 일시적인 `UnknownTopicOrPartitionException`은 준비 제한 안에서 조회만 재시도합니다. 준비 실패는 실패 기록·복구 cleanup 후 다음 실험으로 진행하되, 복구 실패나 시작 전 안전 검사 실패 시 전체 실행을 중단합니다.

토픽은 100개 단위 Admin API 요청으로 생성하며 파티션 수와 복제 계수는 브로커 기본값을 사용합니다. 브로커의 `auto.create.topics.enable=true`와 `false` 모두 지원하고 설정을 변경하지 않습니다. 토픽 생성·조회·쓰기·삭제 권한이 필요합니다.

| 설정 Hz | 20분 사전 생성 토픽 수 | 준비 메시지 수 | 수명 40초의 안정 상태 활성 생산 토픽 목표 |
| --- | ---: | ---: | ---: |
| 1 | 1,200 | 1,200 | 40 |
| 2 | 2,400 | 2,400 | 80 |
| 3 | 3,600 | 3,600 | 120 |
| 4 | 4,800 | 4,800 | 160 |
| 5 | 6,000 | 6,000 | 200 |

토픽 수는 `--topic-cnt -1`일 때 `ceil(Hz × 60 × 실행 분)`이며 양수 지정 시 그 값을 사용합니다. 각 차량은 최대 40초간 100 Hz 생산을 목표로 합니다. Hz는 이미 생성된 토픽의 **생산 시작 속도**이지, 사전 생성 속도가 아닙니다. 시작부터 모든 토픽이 존재하므로 auto 모드와 토픽 수에 따른 브로커 부하는 다릅니다. fio 유무 비교 시 나머지 설정을 동일하게 맞추세요.

## 결과 폴더와 추적

각 블록의 `--output-dir` 아래에 회차별 폴더를 자동 생성합니다.

```text
<실행ID>_0_1Hz_mode-precreate_fio-on_delete-off_20min_start_<시작KST>_KST/
```

`--retry 2` 이상이면 반복 번호도 포함합니다. CSV·로그 파일명에는 회차 시작·종료 KST 시각이 들어가며 실행 중에는 종료 부분이 `end_PENDING`입니다. 종료 시각은 cleanup·대기까지 포함합니다. 터미널에는 현재 KST가 붙고 iostat은 1초 간격으로 별도 기록합니다. `.run.txt`에는 실행 인자·실제 명령·결과 경로·상태를, 출력 루트의 `_runs.tsv`에는 연결표를 기록합니다.

세부 옵션은 [실행 안내](AUTO_METRICS_USAGE.md)를 참고하세요. 아래 명령의 실제 Kafka/fio 부하는 문서 작성·검증 과정에서 실행하지 않았습니다.
