# Kafka 실험 실행 방법

실험 모드에서는 각 Hz/반복 시작 전에 `fstrim`이 기본 실행됩니다. `--fstrim-target-dir /data4 --fstrim-wait-seconds 600`으로 대상과 완료 후 대기를 지정하며, 대기시간은 최소 600초(10분)입니다. 예를 들어 `1200`은 20분입니다. `--no-fstrim`이면 TRIM과 대기를 모두 생략합니다. 단독 fio/delete/cleanup에는 기본 적용하지 않습니다. TRIM은 시작 전 토픽 정리·충돌 검사 이후, 사전 생성과 fio 이전에 실행하며 실패하면 해당 회차에서 생산을 시작하지 않습니다. 대상 파일시스템의 장치를 `--expected-disk`와 대조하고, 일반 계정은 `sudo -n fstrim` 권한이 필요합니다. `.run.txt`와 `_fstrim.log`에 설정·명령·완료/대기 이벤트를 남깁니다. 1~5Hz 각 1회면 TRIM 후 대기만 최소 총 50분 추가됩니다.

생산기를 .42에서 실행하려면 [분산 실행 안내](KAFKA_REMOTE_EXECUTION.md)의 `/home/hss544/kafka-test-clients/run_remote_kafka_experiments.sh`를 사용하세요. 토픽 생성·데이터 생산·삭제 요청은 .42, build/cache drop/fstrim/fio/iostat은 .11에서 실행합니다.

각 Hz/반복 회차의 결과는 `--output-dir` 아래 별도 폴더에 저장합니다. 폴더명은 고유 실행 ID·Hz·반복 번호·토픽 모드·fio·delete·설정 시간·실제 회차 시작 KST 시각을 포함합니다. 예: `<실행ID>_0_1Hz_mode-precreate_fio-off_delete-off_20min_start_20260930_140000_KST`. 삭제 ON은 `delete-on-batch` 또는 `delete-on-immediate`로 표시합니다. `20min`은 `--run-duration-minutes` 설정값이며 준비·cleanup을 포함한 실제 소요 시간이 아닙니다. 실행 중 파일명은 `_start_YYYYMMDD_HHMMSS_KST_end_PENDING`을 사용하고, 정상·실패·중단 처리 종료 후 `_end_YYYYMMDD_HHMMSS_KST`로 확정합니다. 종료 시각에는 cleanup·회복 처리·설정된 대기 시간이 포함됩니다. CSV와 로그 내용은 유지하며 `.run.txt`의 최종 결과 경로와 루트 `_runs.tsv` 연결표를 갱신합니다. 실제 실행 명령은 당시 임시 경로 그대로 보존하고 `ARTIFACT_RENAME`으로 최종 이름을 연결합니다. SIGKILL/전원 장애에는 종료 처리가 불가능해 `PENDING`이 남습니다. 공통 빌드 로그와 연결표는 출력 루트에 유지합니다.

`--topic-mode precreate`는 전체 토픽 생성 및 리더/ISR 확인 후 **각 토픽에 준비 메시지 1개**를 `--msg-size` 크기로 전송하고 `acks=all` 응답을 기다립니다. 이후 선택적 fio와 본 실험을 진행합니다. 준비 메시지는 생산 CSV 통계에서 제외하고 `_precreate.log`에 응답 완료 개수를 기록합니다. `.run.txt`에는 `PRECREATE_SEED_*` 설정을 기록합니다. 준비 전송 실패도 사전 생성 실패로 처리하여 cleanup 후 기존 실패 처리 정책을 따릅니다. 생성·준비 전송은 하나의 `--topic-prepare-timeout-seconds` 제한을 공유합니다. 준비 메시지는 실제 토픽에 남으므로 본 실험 시작 시 빈 토픽이 아니며, 디스크 사용량과 오프셋에는 반영됩니다. 생산 직전 토픽 재확인에서는 준비 메시지를 다시 보내지 않습니다.

터미널 출력에는 `[YYYY-MM-DD HH:MM:SS KST]`를 붙이고 실행 이벤트에는 UTC와 KST를 함께 기록합니다. 하위 도구의 원본 로그 형식과 CSV 컬럼은 유지합니다. 배포 시 Python 3와 `tools/kst_console.py`, `tools/finalize_experiment.py`가 필요합니다.

빌드는 `--build`가 기본입니다. `--no-build`를 지정하면 기존 `build/libs/apps-1.0-SNAPSHOT-all.jar`를 재사용하며 Gradle을 실행하지 않습니다. 읽을 수 있는 비어 있지 않은 JAR가 없으면 실험 시작 전에 실패합니다. 소스와 JAR의 최신성은 자동 판정하지 않으므로 Java 변경 후에는 `--build`로 실행하세요. 단독 모드에도 적용됩니다. 삭제기 클래스의 별도 `javac` 컴파일은 유지됩니다. `.run.txt`에 `BUILD_ENABLED`, `_build.txt`에 `SKIPPED`와 재사용 JAR 해시를 기록합니다.

## 단계 간격·캐시·토픽 준비 재시도

빌드와 전체 실험 스크립트는 일반 계정 `hss544`로 실행하세요. `sudo bash run_kafka_experiments.sh ...`처럼 전체를 root로 실행하면 의존 프로젝트의 빌드 산출물까지 root 소유가 되어 다음 일반 계정 빌드가 실패할 수 있습니다. `--drop-caches`는 필요한 캐시 작업에만 내부적으로 `sudo -n`을 사용하므로 전체 스크립트를 sudo로 실행할 필요가 없습니다.

- `--step-delay-seconds 10`: 실제 실행하는 단계 사이에 기본 10초 대기합니다. `0`이면 끕니다. 생략한 fio/delete 단계와 단순 기록 출력에는 별도 대기를 추가하지 않습니다. 시작 전 정리, 충돌 확인, 사전 생성, fio, 삭제기 시작, 생산, 결과 확인, 종료 정리 및 캐시 작업 사이에 적용하며 1초마다 진행을 표시합니다. 실험 사이의 `--inter-experiment-delay-seconds` 정리·대기 시간과는 별도입니다.
- `--drop-caches`: 실험 모드의 기본값입니다. 각 Hz/반복 회차의 시작 전과 종료 cleanup·대기 후 실행 서버에서 `sync` 후 `/proc/sys/vm/drop_caches`에 `3`을 씁니다. 페이지 캐시와 dentry/inode 캐시를 비우며 JVM 힙, 프로세스 메모리, SSD 내부 캐시는 초기화하지 않습니다. 서버 전체 캐시에 영향을 줍니다. 생산 구간 직전에 추가로 비우는 옵션은 아닙니다.
- `--no-drop-caches`: 캐시 작업을 명시적으로 끕니다. 비 root 계정에서 활성화하려면 sudo 권한이 필요합니다. 스크립트는 `sudo -n`을 사용하므로 필요하면 같은 터미널에서 `sudo -v` 후 실행하세요. 장시간 실행 중 권한 인증이 만료되거나 실제 명령이 거부되면 해당 캐시 단계가 실패합니다.
- 실패 회차도 복구 cleanup 성공 후 캐시를 비웁니다. 복구 cleanup 실패 또는 사용자 중단 시에는 후처리 캐시 비우기를 보장하지 않습니다. 시작 전 준비 실패는 전처리 캐시 작업이 성공한 경우 후처리 캐시 작업을 수행합니다.
- 설정은 `.run.txt`의 `STEP_DELAY_SECONDS`, `DROP_CACHES`에 저장합니다. 캐시 명령·성공/실패는 실행 기록과 `<tag>_drop_caches_before.log`, `_after.log`, `_after_failure.log`에서 확인합니다.
- precreate의 `UnknownTopicOrPartitionException`은 최대 1초 간격으로 조회를 재시도합니다. 기존 `--topic-prepare-timeout-seconds`는 모든 배치 및 재시도를 합한 전체 준비 제한입니다. 생성 요청 자체와 다른 종류의 오류는 재시도하지 않습니다. `--retry`는 계속 실험 총 반복 횟수를 의미합니다.

실험 모드에서는 캐시 비우기가 기본 ON입니다. `--no-drop-caches`를 명시하면 끌 수 있습니다. 환경 변수 `DROP_CACHES=false`도 적용되지만 CLI 옵션이 우선합니다. 독립 도구인 `--mode fio|delete|cleanup`에는 실험 전후 단계가 없으므로 기존처럼 캐시 비우기를 수행하지 않습니다.

## 서버 배치

다음 파일을 모두 `/data4/kafka-test-clients/`에 배치합니다.

- `run_kafka_experiments.sh`: 통합 실행기 (기존 run_auto_topic_producer_metrics.sh)
- `prepare_ssd_fio.sh`: SSD 사전 준비 (기존 disk_saturation.sh)
- `KafkaJmxProducerBatchDaemon.java`: 실시간 비활성 삭제 및 일회성 정리

Java 파일명은 public 클래스 이름과 같아야 하므로 유지했습니다. 기존 run.sh, delete.sh는 통합 실행기에서 호출하지 않습니다. Kafka 라이브러리는 기본 /data4/kafka-tt, 생산 실행기는 /data4/kafka-test-clients/scripts/kafka_clients_run.sh입니다.

```bash
cd /data4/kafka-test-clients
chmod +x run_kafka_experiments.sh prepare_ssd_fio.sh
./run_kafka_experiments.sh --help
```

## 실행 예시

```bash
# fio 준비와 시간 기반 삭제를 사용하여 1~5 Hz 실험
./run_kafka_experiments.sh --build --step-delay-seconds 10 --drop-caches --all --fio --delete

# 생산만 3 Hz 실행
./run_kafka_experiments.sh --build --step-delay-seconds 10 --drop-caches --hz 3 --no-fio --no-delete --cleanup

# 각 실험 10분, 비활성 60초 후 삭제, 실험 사이 30초 대기
./run_kafka_experiments.sh --build --step-delay-seconds 10 --drop-caches --all --delete \
  --run-duration-minutes 10 --inactivity-threshold-ms 60000 \
  --inter-experiment-delay-seconds 30

# fio 준비만 수행
./run_kafka_experiments.sh --build --step-delay-seconds 10 --no-drop-caches --mode fio --fio-target-dir /data4 \
  --fio-passes 5 --reserve-gib 4 --expected-disk nvme1n1

# 실시간 삭제만 수행 (Ctrl+C로 종료)
./run_kafka_experiments.sh --build --step-delay-seconds 10 --no-drop-caches --mode delete \
  --delete-topic-prefix test_creator_ \
  --brokers 127.0.0.1:9092 --jmx-host 127.0.0.1

# 공통 토픽 이름의 잔여 토픽을 수동 정리: 이 범위 전체가 삭제 대상임을 확인 후 실행
./run_kafka_experiments.sh --build --step-delay-seconds 10 --no-drop-caches --mode cleanup \
  --cleanup-topic-prefix test_creator_0_

# SSD 준비 스크립트를 직접 실행
./prepare_ssd_fio.sh --target-dir /data4 --size-gib auto \
  --passes 3 --reserve-gib 2 --expected-disk nvme1n1
```

## 옵션

값이 필요한 옵션은 `--옵션 값`과 `--옵션=값` 모두 지원합니다. CLI 설정은 같은 이름의 환경 변수보다 우선합니다. 기능 플래그에는 값을 붙이지 않습니다.

| 기능 | 켜기 / 끄기 | 기본값 |
|---|---|---|
| fio 준비 | --fio / --no-fio | 켜짐 |
| 실시간 삭제 | --delete / --no-delete | 꺼짐 |
| 실험 사이 즉시 정리 (시간 조건 없음) | --cleanup (실험 모드 필수) | 켜짐 |
| 생산 통계 저장 | --stats-enabled / --stats-disabled | 저장 |

- 실행 선택: `--all`, `--hz 3`, `--mode fio|delete|cleanup|experiment` 중 하나. 미지정 시 --all. 단독 모드는 해당 기능의 on/off 플래그와 무관하게 실행합니다.
- 생산: `--brokers` (166.104.110.11:9092), `--prefix` (test_creator), `--topic-cnt` (-1), `--run-duration-minutes` (60), `--topic-lifetime-ms` (40000), `--produce-hz-per-topic` (100), `--msg-size` (4096), `--start-index` (0), `--start-barrier-delay-ms` (10000).
- 생산 플래그: `--ignore-response`, `--need-flush`, `--share-producer`. 그 외 Java 옵션은 `--` 구분자 뒤에 전달할 수 있습니다. 자동 관리하는 접두어, Hz, CSV 경로는 중복 지정할 수 없습니다.
- fio: `--fio-target-dir` (/data4), `--fio-size-gib` (auto), `--fio-passes` (3), `--reserve-gib` (2), `--expected-disk` (nvme1n1), `--fio-script` (실행기 옆 prepare_ssd_fio.sh). fio 출력은 터미널과 `*_fio.log`에 동시에 기록됩니다.
- 감시: `--jmx-host` (127.0.0.1), `--jmx-port` (9999), `--scan-interval-ms` (1000), `--scan-log-interval-ms` (1000), `--inactivity-check-interval-ms` (1000), `--inactivity-threshold-ms` (600000), `--deletion-mode` (BATCH 또는 IMMEDIATE), `--delete-start-timeout-seconds` (60).
- 삭제 범위: `--delete-topic-prefix` 기본값은 `PREFIX_` (기본 test_creator_). 실행 ID와 무관하게 이 범위에서 비활성 시간이 지난 토픽을 삭제합니다. `--cleanup-topic-prefix`는 단독 즉시 정리 범위입니다. 정상/실패 cleanup은 `PREFIX_0_인덱스` 중 해당 회차의 시작 인덱스와 토픽 수에 해당하는 이름만 선택합니다.
- 정리: `--cleanup-timeout-seconds` (300), `--inter-experiment-delay-seconds` (60, 0이면 대기 없음).
  - cleanup은 최대 1,000개씩 순차 요청합니다. 예를 들어 18,000개 토픽은 18개 요청으로 분할하여 단일 요청의 10,000개 초과 문제를 피합니다. 실험 중 BATCH/IMMEDIATE 방식은 변경하지 않습니다.
  - 이미 없는 토픽의 `UnknownTopicOrPartitionException`만 허용하고, 모든 요청 이후 대상 토픽이 목록에서 사라졌는지 확인해야 성공합니다. 권한 오류 등 다른 실패는 그대로 전달합니다.
  - 정리 제한 시간은 모든 배치와 최종 확인에 공유되며 배치마다 초기화되지 않습니다. 대규모 정리에 300초가 부족하면 `--cleanup-timeout-seconds 600`처럼 명시적으로 늘리세요. 시작 전·종료 후·실패 복구 cleanup에 동일하게 적용됩니다.
- 경로: `--output-dir` (실행기 옆 renewed_output/auto_topic_producer_metrics), `--kafka-home` (/data4/kafka-tt), `--daemon-source` (실행기 옆 Java 파일), `--client-dir`, `--runner`.

## 실행 순서

터미널의 STEP 0~6 설명은 CLI/환경 변수/기본값을 반영합니다. 단계별 사용 여부 및 생략 이유, fio 용량·반복 횟수·예약 공간, 삭제 주기·기준·접두어, 생산 토픽 수·부하, cleanup 범위·제한 시간·남은 대기 시간을 표시합니다. 같은 설명을 `.run.txt`에 UTC 시각과 함께 저장합니다. fio `auto`의 실제 계산 용량은 fio 출력에서 확인합니다.

1. 옵션·의존성을 확인하고 사용할 삭제 데몬을 컴파일합니다. 단일 Hz 또는 1~5 Hz 실행 목록을 정합니다.
2. fio가 켜져 있으면 가용 공간을 다시 계산하고 예약 공간을 제외한 파일을 채워 반복 덮어씁니다. fio 종료와 임시 파일 삭제를 확인합니다.
3. delete가 켜져 있으면 JMX에 연결하여 관리 접두어에 해당하는 토픽을 조회합니다. 준비 완료 후 생산을 시작합니다.
4. 생산은 지정 Hz로 토픽 작업자를 시작합니다. 동시에 삭제기는 오프셋 증가를 관측할 때마다 비활성 타이머를 갱신합니다. `현재 시각 - 마지막 증가 관측 시각 >= --inactivity-threshold-ms`이면 삭제 대상으로 넣습니다. BATCH는 한 검사에서 발견한 대상을 묶고 IMMEDIATE는 개별 요청합니다.
5. 모든 생산 작업자가 종료하고 CSV를 기록하면 삭제기도 종료합니다. 실험 중 생성한 파일과 삭제 로그는 실행 ID/Hz별로 분리합니다.
6. 단일/마지막 Hz를 포함해 현재 실험 토픽을 매번 정리합니다. 정리 시간은 inter-experiment-delay-seconds에 포함됩니다. 동일 이름 재사용을 위해 실험 모드에서는 --no-cleanup을 거부합니다.
7. 정리·대기 구간의 남은 시간만 기다리고 iostat을 종료합니다. 다음 실험이 있으면 2단계부터 반복합니다. 정리가 딜레이보다 오래 걸리면 완료까지 기다리며 cleanup-timeout-seconds 초과 시 실패로 중단합니다.

실행 ID는 생산 토픽 이름과 결과 파일 구분용으로 남아 있으며 시간 기반 삭제의 판정 조건이 아닙니다. `--delete`/`--no-delete`, `--mode delete`, `--delete-topic-prefix`, `--delete-start-timeout-seconds`를 사용합니다. 기존 monitor 옵션 이름은 사용하지 않습니다.

예: 마지막 증가가 12:00:40에 관측되고 비활성 기준이 60초이면 약 12:01:40 이후 검사에서 삭제 후보가 됩니다. 생성 시각 기준 TTL은 아닙니다. 최초 오프셋 관측부터 타이머를 시작하므로, 데몬을 재시작하면 기존 토픽의 이전 비활성 시간은 복구하지 않습니다. 실험 사이 대기 및 fio 준비 동안 삭제기는 실행되지 않으며, 시간 조건을 만족하지 않은 토픽도 cleanup이 켜져 있으면 생산 종료 후 정리됩니다. 이전 실험 토픽이 남을 수 있으므로 기본 시간 기반 삭제만으로 브로커 상태가 완전히 초기화되지는 않습니다.

단일/마지막 실험 뒤에도 cleanup 및 정리·대기를 적용합니다. 실행 ID와 Hz별로 CSV·로그를 분리합니다. 실험 도중 실패하면 해당 실험의 나머지 단계를 건너뛰고 아래 실패 복구 절차를 수행합니다. 종료 시 실행기가 시작한 프로세스 그룹에 정상 종료를 요청하고 최대 10초 후 남은 프로세스를 강제 종료합니다.

JMX와 브로커 주소는 같은 클러스터를 가리켜야 합니다. 단일 브로커 JMX 감시는 전체 클러스터 활동 집계가 아닙니다. 별도로 실행한 기존 삭제 데몬은 이 실행기가 관리하지 않습니다. fio 사전 쓰기는 실험 내내 SSD GC가 지속됨을 보장하지 않습니다.

## 실행 조건 기록 및 명령 문서

통합 스크립트는 기본값인 --build에서 매 호출(단독 fio/delete/cleanup 모드 포함) 시작 시 `--client-dir` 프로젝트의 `gradlew -p <프로젝트> :apps:shadowJar --rerun-tasks --no-build-cache --no-daemon --console=plain`을 수행합니다. `--all`과 `--retry` 내부에서는 재빌드하지 않으며 모든 회차가 같은 JAR을 사용합니다. `--help`는 빌드하지 않습니다. 빌드 실패나 산출 JAR 누락 시 실험을 시작하지 않고 종료합니다. 실험 구간에 빌드 부하를 넣지 않도록 fio/iostat/토픽 준비 전에 완료합니다.

빌드는 시작·1초별 경과·종료 코드만 터미널과 `<실행ID>_build.log`에 남깁니다. Gradle의 원본 출력(태스크/경고/오류 상세)은 터미널 및 상세 로그에 저장하지 않습니다. 명령·성공/실패·JAR 해시는 `<실행ID>_build.txt`에 남깁니다. 각 `.run.txt`에도 빌드 기록 경로와 JAR 해시를 기록합니다. 서버에는 Gradle wrapper, 설정 파일, apps/core 소스 및 의존 프로젝트가 필요합니다. 현재 `local.settings.gradle`은 `../kafka-tt`, `../moniq`를 참조합니다. 과거 상세 빌드 로그는 삭제하지 않습니다.

- 사전 생성 실험: [KAFKA_PRECREATED_SCENARIOS.md](KAFKA_PRECREATED_SCENARIOS.md). `--topic-mode precreate --fio`와 `--topic-mode precreate --no-fio`로 토픽 생성 → 선택적 fio → 데이터 생산 → 정리를 선택합니다. 수정된 생산기 JAR가 필요하며 기본 --build에서는 실행할 때 자동 재빌드합니다. 브로커의 `auto.create.topics.enable=true`와 `false` 모두 지원하며 설정을 변경하지 않습니다. 기본 `auto`는 기존 방식을 유지합니다.

- `KAFKA_EXPERIMENTS_SEQUENTIAL.md`: 조건별 1~5 Hz 순차 실행 및 전체 조합 자동 실행.
- `KAFKA_EXPERIMENTS_SINGLE_HZ.md`: 각 조건의 1/2/3/4/5 Hz를 별도로 실행하는 명령.
- `<tag>.run.txt`: 원본 CLI, 적용 설정, 실제 생산/fio/삭제 명령, CSV 경로, 단계별 UTC 시각 및 종료 상태. 동일 태그의 `<tag>_metrics.csv`와 `<tag>_deletion.csv`에 대응합니다.
- `<실행ID>_runs.tsv`: 한 번의 실행에 속하는 Hz별 CSV와 메타데이터 경로 연결표.

CLI를 생략해 환경 변수나 기본값이 적용되어도 적용값을 기록합니다. 기존 CSV 내부 컬럼은 변경하지 않으며, 업데이트 이후의 실행부터 기록합니다. 경로 기록은 파일 생성 성공을 의미하지 않으므로 종료 상태와 실제 CSV 존재를 함께 확인하세요.

## 검증 범위

### 같은 실험 반복 (`--retry`)

`--retry N`은 각 Hz의 총 실행 횟수입니다. 생략하면 1회이며, 실패한 작업을 자동 재시도하는 옵션은 아닙니다. 1~9999의 정수를 받으며 환경 변수 `RETRY`보다 CLI가 우선합니다. 단독 fio/delete/cleanup 모드에서는 1만 허용합니다.

```bash
cd /data4/kafka-test-clients
# 3 Hz 실험을 3회 반복 (나머지 실험 옵션을 뒤에 추가할 수 있습니다).
bash ./run_kafka_experiments.sh --build --step-delay-seconds 10 --drop-caches --hz 3 --retry 3
# 각 Hz를 2회씩: 1→1→2→2→3→3→4→4→5→5 Hz
bash ./run_kafka_experiments.sh --build --step-delay-seconds 10 --drop-caches --all --retry 2
```

각 반복은 활성화된 fio 사전 준비, 삭제기, 생산, 필수 cleanup과 정리·대기를 모두 수행합니다. 같은 Hz의 반복 사이에도 `--inter-experiment-delay-seconds`를 적용하며 cleanup 시간이 이 시간에 포함됩니다. 실패하면 cleanup 복구 성공 후 다음 예정 반복/Hz로 진행합니다.

### 실패 후 복구 및 다음 실험

1. 실패 원인과 종료 코드를 `<tag>_failure.log`와 `<tag>.run.txt`에 남깁니다. Java/fio의 상세 출력은 기존 단계별 로그에 보존합니다.
2. 해당 실행의 생산기·삭제기·iostat을 종료합니다. 복구 구간에는 iostat을 수집하지 않습니다.
3. 현재 회차의 `PREFIX_0_인덱스` 범위만 cleanup합니다. 복구 로그는 `<tag>_recovery_cleanup.log`에 저장하여 기존 cleanup 로그를 보존합니다. 정상 cleanup에서 실패한 경우도 복구 cleanup을 한 번 시도합니다. 시작 전 준비/검사 실패는 추가 복구 삭제 없이 전체 실행을 중단합니다. precreate의 시작 전 정리가 실패한 경우 이미 일부 토픽이 삭제됐을 수 있습니다.
4. 복구 cleanup 시작부터 `--inter-experiment-delay-seconds`를 채운 후 다음 예정 실험으로 진행합니다. cleanup 시간이 더 길면 추가 대기는 없습니다. 실패한 실험을 다시 실행하거나 `--retry` 횟수를 늘리지는 않습니다.
5. 복구 성공은 `FAILED_CLEANED`, 복구 실패는 `RECOVERY_FAILED`로 기록합니다. 복구 실패 시에는 토픽이 남을 수 있으므로 전체 실행을 중단합니다. 옵션·의존성 사전 검사 실패와 사용자 중단도 다음 실험으로 넘어가지 않습니다.

실험 모드에서는 실패 복구를 위해 삭제·cleanup OFF여도 Java cleanup 의존성을 준비합니다. 모든 예정 실험을 수행하더라도 실패가 하나 이상 있으면 최종 종료 코드는 1입니다. 따라서 외부에서 `set -e`로 여러 실행 명령을 묶었다면 해당 명령이 끝난 후 다음 명령은 실행되지 않을 수 있습니다. 단독 fio/delete/cleanup 모드는 자동 다음 실험 기능의 대상이 아닙니다.

결과 파일 태그에는 `_repeat1`, `_repeat2` 등을 붙여 CSV·로그 덮어쓰기를 방지합니다. **토픽 접두어에는 실행 ID·Hz·반복 번호를 붙이지 않습니다.** 토픽은 모든 회차에서 `PREFIX_0_인덱스` 이름을 재사용합니다. precreate에서는 해당 회차의 정확한 이름 범위를 시작 전에 삭제하고 삭제 완료를 확인합니다(기존 데이터도 삭제). 시작 전 cleanup 로그/CSV는 `<tag>_before_precreate_cleanup.log/.csv`이며 명령과 범위를 `.run.txt`에 기록합니다. auto는 기존 이름 발견 시 생성·생산·삭제 없이 중단합니다. `.run.txt`에 `RETRY`와 `RETRY_INDEX`를 저장하고 실행 목록에도 반복 번호를 기록합니다. 같은 접두어로 실험을 동시에 실행하지 마세요.

STEP별 진행 상태는 자동으로 터미널과 `<tag>.run.txt`에 기록됩니다. 추가 옵션은 필요하지 않습니다.

- STEP 2: 삭제기 준비 중에는 1초마다 경과/시작 제한 시간과 최근 로그를 표시합니다. 준비 완료 후 생산 중에도 1초마다 삭제기 PID, 조회 간격, 삭제 방식과 최근 로그를 표시합니다. `--no-delete`이면 생략 사유를 표시합니다.
- STEP 3: 삭제 기능의 ON/OFF와 관계없이 1초마다 경과 시간과 남은 시간만 표시합니다. 남은 시간은 `--run-duration-minutes` 기준이며 실제 종료 예상 시간이 아닙니다(토픽 수 직접 지정/추가 인수 적용 시에도 동일). 0초 이후에도 생산 완료를 기다립니다. 상세 생산 로그는 `<tag>_producer.log`에 유지하며, 오류는 별도 표시합니다. 병행하는 STEP 2의 삭제 상태 표시는 유지됩니다.
- STEP 5: cleanup 중에는 1초마다 경과 시간과 최근 cleanup 로그를 표시합니다. 로그의 LISTING/DELETE_REQUEST/VERIFY는 목록 조회/삭제 요청/삭제 확인 단계이며 VERIFY에는 남은 토픽 수가 포함됩니다. 정리 후 남은 대기도 1초마다 표시합니다. cleanup 시간도 설정한 딜레이에 포함됩니다.

최근 로그는 Java가 마지막으로 출력한 기록이므로 매 표시마다 바뀌지는 않습니다. 프로세스 실행 상태는 실제 토픽 생성·삭제의 진척이나 응답성을 보장하지 않으며, 완료 비율을 추정하지 않습니다. 상태 표시를 위한 별도 Kafka/JMX 조회는 추가하지 않습니다.

Bash 구문 및 모의 호출 순서/기능 스위치/실패 중단/단독 실행을 검증했습니다. 실제 Kafka/JMX 연결, Java 컴파일, SSD 쓰기, Linux 프로세스 그룹 종료는 서버 환경에서 확인해야 합니다.

## iostat 1초 디스크 상태 로그와 정리 시간

- iostat은 기본 켜짐이며 `--iostat` / `--no-iostat`로 제어합니다. 서버에 sysstat 패키지의 iostat이 필요합니다.
- `iostat -d -x -t -y 1`로 전체 디스크의 확장 I/O 통계를 1초 간격으로 기록합니다. 부팅 이후 누적 첫 보고서는 생략합니다. fio 직전부터 생산 및 정리·대기 종료까지 `<tag>_iostat.log`에 저장합니다.
- `.run.txt`의 IOSTAT_COMMAND, IOSTAT_LOG와 `_runs.tsv`의 iostat_log 열에서 CSV와 연결할 수 있습니다. 단계별 UTC 기록의 FIO, PRODUCER, CLEANUP, COOLDOWN_START/END로 구간을 구분합니다.
- 예: 딜레이 60초, cleanup 15초이면 남은 45초만 대기합니다. cleanup 80초이면 80초 후 진행합니다. 딜레이 0이면 추가 대기는 없지만 cleanup 완료까지 기다립니다. 단독 `--mode cleanup`은 요청된 즉시 정리만 수행하며 실험 딜레이는 적용하지 않습니다.
- 실시간으로 보려면 별도 터미널에서 `tail -f /실제/결과경로/실제태그_iostat.log`를 사용하세요.
- iostat 옵션 설명: [Linux iostat 매뉴얼](https://www.man7.org/linux/man-pages/man1/iostat.1.html).
