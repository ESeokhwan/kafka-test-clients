# .42 → .11: 삭제 OFF/BATCH/IMMEDIATE × fio OFF/ON

조회·비활성 검사 간격을 1초/30초/1분/5분으로 함께 바꾸는 네 버전과 전체 시간 추정은 [간격별 실행 문서](KAFKA_CLIENT42_INTERVAL_MATRIX.md)를 참고하세요. `--intervals all`을 추가하면 네 간격을 모두 순차 실행하며, 생략하면 기존 기본 1초입니다. 결과 폴더에는 `scan_Nms_check_Nms` 계층이 추가되고 matrix.tsv에도 두 간격 열이 기록됩니다.

실행 위치는 **166.104.110.42의 /home/hss544/kafka-test-clients**입니다. 토픽 생성·생산·삭제 요청은 .42에서, 브로커·TRIM·fio는 .11에서 실행합니다. auto 모드로 비교하며 precreate와 실시간 delete의 조합은 현재 통합 실행기에서 지원하지 않습니다.

## 삭제 모드의 의미

| 설정 | 동작 |
| --- | --- |
| OFF | 생산 중 비활성 삭제기를 실행하지 않음. 회차 종료 cleanup은 수행 |
| BATCH | 한 비활성 검사에서 삭제 대상으로 판정된 토픽을 목록으로 묶어 삭제 큐에 전달 |
| IMMEDIATE | 같은 비활성 기준을 충족한 토픽을 각각 단일 토픽 요청으로 삭제 큐에 전달 |

IMMEDIATE도 조회·검사 주기와 비활성 기준을 따릅니다. 비활성 10분 조건을 생략하거나 파일의 즉시 물리 삭제를 보장하는 모드가 아닙니다. BATCH도 실험 종료까지 기다리는 모드가 아닙니다.

## 공통 기본값

- 각 버전: 1~5Hz, 각 Hz 총 3회. 순서는 1Hz×3 → 2Hz×3 → 3Hz×3 → 4Hz×3 → 5Hz×3.
- 회차당 생산 시작 구간 20분, 차량 생산 수명 40초, 차량당 100Hz, 메시지 4096바이트.
- 비활성 기준 600000ms(10분), 조회·비활성 검사 주기 1000ms, 삭제 로그 주기 60000ms.
- 매 회차 .11의 /data4에 TRIM 후 600초 대기. fio ON이면 그 뒤에 fio를 실행하며 fio 직후에는 TRIM하지 않습니다.
- fio ON: /data4 가용 공간 중 2GiB를 남긴 임시 파일에 3회 쓰기 후 삭제, 예상 장치 nvme1n1.
- cleanup·생산 통계 ON, build/cache drop/iostat OFF. 단계 간 10초, cleanup 포함 종료 정리 구간 최소 60초.
- 삭제 ON에는 .42 → .11 JMX/RMI 접근이 필요합니다. 기본 166.104.110.11:9999이며 TCP 연결 확인만으로 전체 RMI 통신을 검증할 수는 없습니다.
- 동일 브로커와 test_creator 토픽 이름을 사용하므로 동시에 실행하지 마세요. 이 비교 실행기는 다른 비교 실행기를 잠금으로 막지만 기존 실행기의 직접 실행까지 막지는 않습니다.

## 여섯 개별 버전

각 블록은 해당 버전만 실행하며 다른 블록과 독립적입니다.

### 삭제 OFF / fio OFF

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 삭제 OFF / fio OFF / 1~5Hz 각각 3회 / 회차당 20분
  bash ./run_client42_delete_off_fio_off.sh \
    --all --retry 3 --run-duration-minutes 20 \
    --inactivity-threshold-ms 600000 \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --fstrim-wait-seconds 600
)
```

### 삭제 BATCH / fio OFF

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 삭제 BATCH / fio OFF / 1~5Hz 각각 3회 / 회차당 20분
  bash ./run_client42_delete_batch_fio_off.sh \
    --all --retry 3 --run-duration-minutes 20 \
    --inactivity-threshold-ms 600000 \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --fstrim-wait-seconds 600
)
```

### 삭제 IMMEDIATE / fio OFF

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 삭제 IMMEDIATE / fio OFF / 1~5Hz 각각 3회 / 회차당 20분
  bash ./run_client42_delete_immediate_fio_off.sh \
    --all --retry 3 --run-duration-minutes 20 \
    --inactivity-threshold-ms 600000 \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --fstrim-wait-seconds 600
)
```

### 삭제 OFF / fio ON

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 삭제 OFF / fio ON / 1~5Hz 각각 3회 / 회차당 20분
  bash ./run_client42_delete_off_fio_on.sh \
    --all --retry 3 --run-duration-minutes 20 \
    --inactivity-threshold-ms 600000 \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --fstrim-wait-seconds 600
)
```

### 삭제 BATCH / fio ON

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 삭제 BATCH / fio ON / 1~5Hz 각각 3회 / 회차당 20분
  bash ./run_client42_delete_batch_fio_on.sh \
    --all --retry 3 --run-duration-minutes 20 \
    --inactivity-threshold-ms 600000 \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --fstrim-wait-seconds 600
)
```

### 삭제 IMMEDIATE / fio ON

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 삭제 IMMEDIATE / fio ON / 1~5Hz 각각 3회 / 회차당 20분
  bash ./run_client42_delete_immediate_fio_on.sh \
    --all --retry 3 --run-duration-minutes 20 \
    --inactivity-threshold-ms 600000 \
    --scan-interval-ms 1000 --inactivity-check-interval-ms 1000 \
    --fstrim-wait-seconds 600
)
```

## 여섯 버전 전체 순차 실행

순서: OFF/fio OFF → BATCH/fio OFF → IMMEDIATE/fio OFF → OFF/fio ON → BATCH/fio ON → IMMEDIATE/fio ON. 앞 버전의 모든 Hz·반복·cleanup을 마친 후 다음 버전을 시작합니다.

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 6개 버전 × 5개 Hz × 3회 = 총 90회
  # --deletion-mode를 생략해야 BATCH와 IMMEDIATE를 모두 실행
  bash ./run_client42_comparison.sh \
    --variant all --all --retry 3 --run-duration-minutes 20 \
    --fstrim-wait-seconds 600
)
```

총 90회에서 생산 시작 구간과 TRIM 후 대기만 합쳐 45시간이며, fio·준비·cleanup 시간이 추가됩니다.

## BATCH만 포함해 네 조합 실행

삭제 OFF와 BATCH를 fio OFF/ON에 대해 비교합니다. IMMEDIATE는 생략합니다.

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  bash ./run_client42_comparison.sh \
    --variant all --all --retry 3 --run-duration-minutes 20 \
    --deletion-mode BATCH --fstrim-wait-seconds 600
)
```

## IMMEDIATE만 포함해 네 조합 실행

삭제 OFF와 IMMEDIATE를 fio OFF/ON에 대해 비교합니다. BATCH는 생략합니다.

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  bash ./run_client42_comparison.sh \
    --variant all --all --retry 3 --run-duration-minutes 20 \
    --deletion-mode IMMEDIATE --fstrim-wait-seconds 600
)
```

## 특정 Hz만 여섯 버전으로 비교

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 3Hz만 각 버전 1회: 총 6회
  bash ./run_client42_comparison.sh \
    --variant all --hz 3 --retry 1 --run-duration-minutes 20 \
    --fstrim-wait-seconds 600
)
```

## 기존 파일 호환과 옵션

- 기존 `run_client42_delete_on_fio_off.sh`, `run_client42_delete_on_fio_on.sh`는 기본 BATCH 별칭으로 유지합니다. 뒤에 `--deletion-mode IMMEDIATE`를 붙이면 IMMEDIATE 버전을 실행합니다.
- 새 batch/immediate 파일은 이름에 적힌 모드를 고정합니다. 예를 들어 batch 파일에 `--deletion-mode IMMEDIATE`를 붙이면 실행 전 오류로 거부합니다.
- `--variant all`에서 `--deletion-mode`를 생략하면 6개, BATCH 또는 IMMEDIATE를 지정하면 OFF를 포함한 4개 버전을 실행합니다. OFF 케이스는 중복 실행하지 않습니다.
- `--all` 또는 `--hz N` 중 하나만 지정합니다. 생략 시 --all입니다. `--retry`는 실패 재시도 수가 아니라 Hz별 총 회차 수입니다.
- `--run-duration-minutes`, `--fio-passes`, `--scan-interval-ms`, `--inactivity-check-interval-ms`, `--inactivity-threshold-ms`로 해당 조건을 변경합니다.
- `--fstrim-wait-seconds 1200`은 20분 대기, `--no-fstrim`은 TRIM과 대기 생략입니다. 활성화 시 최소 600초입니다.
- `--build`, `--drop-caches`, `--iostat`으로 기능을 켤 수 있습니다. 이 자원 작업은 .11에서 수행합니다.
- 버전이 정하는 delete/fio 스위치, cleanup ON, auto 모드는 직접 변경할 수 없습니다.
- 실행 옵션 확인: `bash ./run_client42_comparison.sh --help`.

## 결과 분리·실패 처리

- `--output-dir ROOT` 아래 `matrix_시작시각_KST_고유값/버전명/`에 저장합니다. BATCH와 IMMEDIATE는 서로 다른 이름의 폴더로 분리됩니다.
- `matrix.run.txt`: 원본 명령과 각 버전에 실제 전달한 인자. `matrix.tsv`: 버전별 시작·종료 KST, 종료 코드, 결과 경로. `matrix.log`: KST 진행 상태.
- 각 버전 폴더 안의 Hz·반복별 CSV, .run.txt, TRIM/fio/delete 로그는 기존 방식대로 저장합니다.
- 회차 실패 후 복구 가능한 경우 하위 실행기는 cleanup 후 남은 회차를 진행합니다. 버전 실행이 최종적으로 실패 종료하면 상위 비교 실행기는 다음 버전을 시작하지 않습니다.
- Ctrl+C/TERM은 현재 하위 실행기에 종료를 전달하고 종료 처리를 기다립니다. 다음 버전은 실행하지 않습니다.
- 문서와 스크립트 배포 검증에서는 실제 TRIM·fio·Kafka 생성·생산·삭제 실험을 실행하지 않습니다.
