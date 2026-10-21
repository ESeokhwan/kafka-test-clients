# .42 → .11: 1초·30초·1분·5분 간격 비교와 소요 시간

최신 축소 조건인 **1초/5분만 + fio 1회 쓰기**는 [축소 실험 및 시간 재계산 문서](KAFKA_CLIENT42_1S_5M_FIO1.md)를 사용하세요. `--intervals 1000,300000 --fio-passes 1`로 지정합니다. 아래 네 간격·fio 3회 조건은 기존 비교용으로 유지합니다.

두 옵션은 각각 토픽 상태 조회 주기와 비활성 삭제 검사 주기입니다. 이 문서의 네 버전은 **두 값을 함께 같은 간격으로 설정**합니다. 서로 다른 값의 4×4 조합은 아닙니다.

| 버전 | --scan-interval-ms | --inactivity-check-interval-ms | 실행 파일 |
| --- | ---: | ---: | --- |
| 1초 | 1000 | 1000 | run_client42_interval_1s.sh |
| 30초 | 30000 | 30000 | run_client42_interval_30s.sh |
| 1분 | 60000 | 60000 | run_client42_interval_1m.sh |
| 5분 | 300000 | 300000 | run_client42_interval_5m.sh |

모든 실행은 **.42의 /home/hss544/kafka-test-clients**에서 진행합니다. 토픽 생성·생산·삭제 요청은 .42, 브로커·TRIM·fio는 .11입니다. 동시에 실행하지 마세요.

## 각 간격별 실행 블록

공통 조건: auto, 삭제 OFF/BATCH/IMMEDIATE × fio OFF/ON의 6개 버전, 각 버전 1~5Hz 각각 총 3회, 회차 생산 시작 구간 20분, TRIM 후 10분 대기. build/cache drop/iostat OFF, cleanup·통계 ON입니다. fio ON은 가용 공간을 사용하여 3회 쓰기 후 임시 파일을 삭제합니다.

### 조회·비활성 검사 모두 1초

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 1초 간격 / 6개 버전 × 5개 Hz × 3회 = 90회
  bash ./run_client42_interval_1s.sh \
    --all --retry 3 --run-duration-minutes 20 \
    --inactivity-threshold-ms 600000 \
    --fstrim-wait-seconds 600
)
```

### 조회·비활성 검사 모두 30초

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 30초 간격 / 6개 버전 × 5개 Hz × 3회 = 90회
  bash ./run_client42_interval_30s.sh \
    --all --retry 3 --run-duration-minutes 20 \
    --inactivity-threshold-ms 600000 \
    --fstrim-wait-seconds 600
)
```

### 조회·비활성 검사 모두 1분

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 1분 간격 / 6개 버전 × 5개 Hz × 3회 = 90회
  bash ./run_client42_interval_1m.sh \
    --all --retry 3 --run-duration-minutes 20 \
    --inactivity-threshold-ms 600000 \
    --fstrim-wait-seconds 600
)
```

### 조회·비활성 검사 모두 5분

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 5분 간격 / 6개 버전 × 5개 Hz × 3회 = 90회
  bash ./run_client42_interval_5m.sh \
    --all --retry 3 --run-duration-minutes 20 \
    --inactivity-threshold-ms 600000 \
    --fstrim-wait-seconds 600
)
```

## 네 간격 전체 순차 실행

순서: 1초의 6개 버전 전체 → 30초의 6개 버전 전체 → 1분의 6개 버전 전체 → 5분의 6개 버전 전체. 각 간격에서 fio OFF의 삭제 OFF/BATCH/IMMEDIATE, fio ON의 삭제 OFF/BATCH/IMMEDIATE 순서입니다. 버전 안에서는 1Hz×3 → 2Hz×3 → … → 5Hz×3입니다.

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 4개 간격 × 6개 버전 × 5개 Hz × 3회 = 총 360회
  bash ./run_client42_comparison.sh \
    --intervals all --variant all --all --retry 3 \
    --run-duration-minutes 20 \
    --inactivity-threshold-ms 600000 \
    --fstrim-wait-seconds 600
)
```

기존 명령에 `--intervals`를 생략하면 기존처럼 기본 1초 간격만 실행합니다. 새 네 간격 전체 비교에는 반드시 `--intervals all`을 붙이세요.

## 3Hz만 네 간격·여섯 버전으로 비교

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  # 4개 간격 × 6개 버전 × 각 1회 = 총 24회
  bash ./run_client42_comparison.sh \
    --intervals all --variant all --hz 3 --retry 1 \
    --run-duration-minutes 20 --fstrim-wait-seconds 600
)
```

## 옵션·기록·해석

- `--intervals 1000|30000|60000|300000`은 하나의 간격만 선택합니다. 네 간격별 파일은 이 옵션을 고정한 진입점입니다.
- `--intervals`와 `--scan-interval-ms` / `--inactivity-check-interval-ms`를 동시에 지정하면 오류로 거부합니다. 서로 다른 두 간격이 필요하면 --intervals를 생략하고 두 개별 옵션을 직접 지정하세요.
- 기존 삭제 모드 필터도 지원합니다. `--deletion-mode BATCH` 또는 IMMEDIATE를 붙이면 각 간격에서 삭제 OFF와 선택 모드 × fio OFF/ON의 4개 버전만 실행합니다.
- 비활성 기준은 모든 간격에서 600000ms(10분)입니다. 조회·검사 주기를 늘리면 관측과 삭제 판정이 더 늦어질 수 있지만, 생산 구간 자체를 늘리지는 않습니다. 실제 삭제 시점은 조회 처리 지연·오프셋 관측 시점·검사 주기의 영향을 받습니다.
- 삭제 OFF에서는 두 간격이 생산 중 삭제에 영향을 주지 않습니다. 완전한 격자 비교를 위해 각 간격에서 OFF 기준 실험도 반복합니다. 따라서 총 360회에는 동일 설정의 OFF 기준 반복이 포함됩니다.
- 결과 경로: `OUTPUT_ROOT/matrix_시작KST_고유값/scan_1000ms_check_1000ms/버전명/...`. 네 간격의 결과가 섞이지 않습니다.
- `matrix.tsv`에도 두 간격의 ms 값을 별도 열로 저장하며, `matrix.run.txt`는 각 간격·버전의 전체 실행 인자를 기록합니다.
- 한 버전이 최종 실패 종료하면 다음 버전·다음 간격은 실행하지 않습니다. 중단과 잠금 동작은 기존 비교 실행기와 같습니다.

## 전체 시간 추정: 기본 360회 기준

| 구성 | 횟수 × 시간 | 합계 |
| --- | --- | ---: |
| 생산 시작 구간 | 360 × 20분 | 120시간 |
| TRIM 완료 후 대기 | 360 × 10분 | 60시간 |
| cleanup을 포함한 종료 정리 구간 | 360 × 최소 1분 | 최소 6시간 |
| 마지막 차량 생산 마무리 | 회차당 약 40초 가정 | 약 4시간 |
| 생산 시작 barrier | 360 × 10초 | 약 1시간 |
| 실제 단계 사이 대기 | 기본 10초, fio/delete 조합별 4~6번 | 약 5시간 10분 |
| fio 실행 | fio ON 180회 | 별도 합산 |
| 기타 | 실제 TRIM·토픽 조회·데몬 시작/종료·SSH·전송 지연·60초 초과 cleanup | 별도 합산 |

**fio 실제 실행시간과 기타 지연을 제외한 계획치는 약 196시간 10분(8일 4시간 10분)**입니다. 이것은 정상 생산 속도와 빠른 cleanup을 가정한 추정이며 고정 종료 시각이나 보장된 하한은 아닙니다. 백프레셔·전송 대기·오류로 달라질 수 있습니다.

fio 한 회차의 평균 시간을 F분이라고 하면:

`전체 예상 시간(시간) ≈ 196.17 + 3 × F + 기타 지연(시간)`

| fio 1회 평균 소요 | fio 180회 합계 | 전체 계획치(기타 지연 제외) |
| ---: | ---: | ---: |
| 15분 | 45시간 | 약 241시간 / 10.0일 |
| 30분 | 90시간 | 약 286시간 / 11.9일 |
| 60분 | 180시간 | 약 376시간 / 15.7일 |
| 120분 | 360시간 | 약 556시간 / 23.2일 |

간격 하나만 실행하면 90회이며, 같은 가정에서 `약 49.04 + 0.75 × F 시간 + 기타 지연`입니다.

### 가용 공간을 전부 쓰는 fio의 영향

가령 fio 파일을 **410GiB**, 반복을 **3회**로 가정하면 한 회차 쓰기량은 약 1230GiB입니다. 실제 가용 공간은 회차마다 다시 계산되며 이 숫자는 예시입니다.

`fio 시간(분) ≈ 파일 GiB × 1024 × passes ÷ 지속 쓰기 MiB/s ÷ 60`

| fio 전체 구간의 평균 쓰기 속도(가정) | fio 1회 계산 시간 | 360회 전체 계획치(기타 지연 제외) |
| ---: | ---: | ---: |
| 1500 MiB/s | 약 14분 | 약 238시간 / 9.9일 |
| 500 MiB/s | 약 42분 | 약 322시간 / 13.4일 |
| 200 MiB/s | 약 105분 | 약 511시간 / 21.3일 |

이전에 측정한 짧은 fio 벤치마크 수치를 위의 지속 쓰기 속도로 단정할 수 없습니다. 현재 준비 fio는 128KiB/QD32로 가용 공간 전체를 여러 번 쓰므로, 실제 준비 로그의 소요시간으로 F를 갱신하는 것이 정확합니다.

기본 원격 단계 제한은 7200초(2시간)입니다. fio가 2시간을 초과하면 위 장기 실험을 끝까지 진행하지 못하고 실패할 수 있습니다. 충분한 단계 제한이 필요한 경우 실행 블록에 `--remote-stage-timeout-seconds 28800`을 추가하면 원격 단계당 최대 8시간입니다. 이 옵션은 실제 fio 시간을 줄이지 않습니다.

실험 횟수를 줄이려면 `--retry 1` 또는 `--hz N`을 선택하세요. 본 문서 작성 및 배포에서는 실제 실험을 실행하지 않습니다.
