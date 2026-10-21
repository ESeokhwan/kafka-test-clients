# .42 → .11: 조회·검사 1초/5분 × fio 1회 쓰기

기존 네 간격 중 **1000ms와 300000ms만** 선택하고, fio ON 회차의 쓰기 횟수를 **1회**로 줄인 버전입니다. 두 옵션(scan-interval-ms, inactivity-check-interval-ms)은 각 케이스에서 같은 값을 사용합니다. fio OFF 회차는 계속 fio를 생략합니다.

## 공통 조건

- .42에서 실행: /home/hss544/kafka-test-clients. 토픽 생성·생산·삭제 요청은 .42, 브로커·TRIM·fio는 .11.
- 간격별 삭제 OFF/BATCH/IMMEDIATE × fio OFF/ON = 6개 버전.
- 각 버전 1~5Hz, 각 Hz 총 3회, 회차 생산 시작 구간 20분.
- 차량 수명 40초, 차량당 100Hz, 메시지 4096바이트.
- 비활성 기준은 간격과 무관하게 600000ms(10분).
- 회차별 TRIM 후 600초 대기, cleanup ON. build/cache drop/iostat OFF.
- fio ON은 /data4 가용 공간에서 2GiB를 남겨 임시 파일에 한 번 쓰고 삭제. 전체 파일 채우기가 1회라는 의미이며 fio 부하가 1초라는 뜻은 아닙니다.
- 같은 브로커·토픽 이름을 사용하므로 블록들을 동시에 실행하지 마세요.

## 1초와 5분을 한 번에 순차 실행

1초의 여섯 버전 전체 → 5분의 여섯 버전 전체 순서입니다. 각 간격 내에서는 fio OFF의 삭제 OFF/BATCH/IMMEDIATE → fio ON의 삭제 OFF/BATCH/IMMEDIATE 순서입니다.

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients

  # 2개 간격 × 6개 조합 × 5개 Hz × 각 3회 = 180회
  # fio는 ON 회차에서만 1회 쓰기 수행: 총 90회
  bash ./run_client42_comparison.sh \
    --intervals 1000,300000 --variant all \
    --all --retry 3 --run-duration-minutes 20 \
    --inactivity-threshold-ms 600000 \
    --fio-passes 1 \
    --fstrim-wait-seconds 600 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_1s_5m_fio1
)
```

위 설정을 고정한 진입점은 `run_client42_intervals_1s_5m_fio1.sh`입니다. 이 파일에 --all/--hz, --retry 등을 추가해서 실행할 수도 있습니다.

## 1초 간격만 실행

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  bash ./run_client42_comparison.sh \
    --intervals 1000 --variant all \
    --all --retry 3 --run-duration-minutes 20 \
    --inactivity-threshold-ms 600000 \
    --fio-passes 1 --fstrim-wait-seconds 600 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_1s_5m_fio1
)
```

## 5분 간격만 실행

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  bash ./run_client42_comparison.sh \
    --intervals 300000 --variant all \
    --all --retry 3 --run-duration-minutes 20 \
    --inactivity-threshold-ms 600000 \
    --fio-passes 1 --fstrim-wait-seconds 600 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/client42_1s_5m_fio1
)
```

전체 순차 블록과 개별 블록은 같은 실험의 대안입니다. 모두 실행하면 실험이 중복됩니다. 결과는 고유 실행 폴더 → scan_Nms_check_Nms → 버전명 → Hz·반복별 폴더로 분리됩니다. matrix.run.txt 및 회차 .run.txt에 간격·fio 횟수·전체 명령이 기록됩니다.

## 소요 시간 재계산

총 실험: 2 × 6 × 5 × 3 = **180회**. 이 중 fio ON **90회**입니다.

| 구성 | 예상 시간 |
| --- | ---: |
| 생산 시작 구간: 180 × 20분 | 60시간 |
| TRIM 후 대기: 180 × 10분 | 30시간 |
| cleanup 포함 기본 정리 구간: 180 × 1분 | 최소 3시간 |
| 마지막 차량 생산 마무리: 약 40초/회차 | 약 2시간 |
| 생산 시작 barrier: 10초/회차 | 약 30분 |
| 단계 간 10초 대기: 조합별 4~6번 | 약 2시간 35분 |
| fio 실행 | 90회의 실제 시간 추가 |
| 실제 TRIM·SSH·조회·데몬 종료·60초 초과 cleanup·전송 지연 | 별도 추가 |

fio와 기타 지연을 제외한 계획치는 **약 98시간 5분(4일 2시간 5분)**입니다. 정상 생산 속도와 빠른 cleanup을 가정한 추정이며 완료 시각을 보장하지 않습니다.

fio 한 회차의 평균 시간을 F분이라고 하면:

`전체 예상 시간(시간) ≈ 98.08 + 1.5 × F + 기타 지연(시간)`

| fio ON 한 회차 평균 | fio 90회 합계 | 전체 계획치(기타 지연 제외) |
| ---: | ---: | ---: |
| 10분 | 15시간 | 약 113시간 / 4.7일 |
| 30분 | 45시간 | 약 143시간 / 6.0일 |
| 60분 | 90시간 | 약 188시간 / 7.8일 |

### 파일 크기 410GiB를 한 번 쓴다고 가정한 예

`fio 시간(분) ≈ 410 × 1024 ÷ 지속 쓰기 MiB/s ÷ 60`

| fio 전체 구간 평균 속도(가정) | fio 한 회차 | 180회 전체 계획치(기타 지연 제외) |
| ---: | ---: | ---: |
| 1500 MiB/s | 약 4.7분 | 약 105시간 / 4.4일 |
| 500 MiB/s | 약 14분 | 약 119시간 / 5.0일 |
| 200 MiB/s | 약 35분 | 약 151시간 / 6.3일 |
| 50 MiB/s | 약 140분 | 약 308시간 / 12.8일 |

410GiB는 예시이며 실제 fio 파일은 회차의 가용 공간으로 결정됩니다. 128KiB/QD32로 파일 전체를 쓰는 지속 성능을 사용해야 하며 앞선 짧은 fio 벤치마크 속도로 단정할 수 없습니다. 현재 조건에서 fio 평균이 약 14~35분이라면 전체 약 **5~6.3일 + 기타 지연**으로 계획할 수 있습니다.

50MiB/s 예시처럼 fio 한 단계가 2시간을 넘으면 기본 원격 제한 7200초에 걸립니다. 필요하면 실행 블록에 `--remote-stage-timeout-seconds 28800`을 추가하세요. 제한을 늘려도 실제 작업시간은 줄지 않습니다.

한 간격만 실행하면 90회·fio ON 45회로 위 전체 추정치의 약 절반입니다. 이전 네 간격·fio 3회 버전에 비해 실험 횟수는 절반, 가용 공간이 같다고 가정한 fio 총 쓰기량은 약 1/6입니다. 속도와 GC 상태가 달라질 수 있으므로 전체 실행 시간이 정확히 1/6이 되는 것은 아닙니다.

이번 문서/배포 검증에서는 실제 Kafka·fio·TRIM 실험을 시작하지 않습니다.
