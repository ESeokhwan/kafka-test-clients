# PRECREATE/OFF 및 AUTO/OFF·BATCH·IMMEDIATE 비교

이 확장은 로컬에서 작성·검증했다. 앞서 서버에 배포한 suite에는 `--scenario-set`과 fstrim 옵션이 없으므로, 아래 명령은 갱신된 `tools/run_experiment_suite.py`를 반영한 뒤 실행해야 한다. 현재 문서 작성 과정에서는 서버를 변경하지 않았다.

## 실행 블록 (.42)

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  bash ./run_client42_suite.sh \
    --scenario-set precreate-auto \
    --hz-list 1 2 3 4 5 \
    --rounds 2 \
    --delete-modes BATCH IMMEDIATE \
    --intervals 1000 300000 \
    --fio-passes 0 \
    --fstrim \
    --fstrim-target-dir /data4 \
    --expected-disk nvme1n1 \
    --fstrim-wait-seconds 600 \
    --run-duration-minutes 60 \
    --topic-prepare-timeout-seconds 600 \
    --step-delay-seconds 10 \
    --inter-experiment-delay-seconds 60 \
    --failure-retries 1 \
    --retry-delay-seconds 60 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results
)
```

순서 확인만 하려면 위 명령에 `--dry-run`을 추가한다. 이 경우 서버 명령이나 실제 대기는 수행하지 않고 계획만 기록한다.

## 각 Hz에서의 순서

| 순서 | 토픽 생성 방식 | 실시간 비활성 삭제 | 조회/삭제 검사 간격 |
|---|---|---|---|
| 1 | PRECREATE | OFF | 해당 없음 |
| 2 | AUTO (CREATE) | OFF | 해당 없음 |
| 3 | AUTO | BATCH | 각각 1초 |
| 4 | AUTO | BATCH | 각각 300초 |
| 5 | AUTO | IMMEDIATE | 각각 1초 |
| 6 | AUTO | IMMEDIATE | 각각 300초 |

1Hz에서 여섯 조건 → 2Hz → 3Hz → 4Hz → 5Hz를 완료하면 한 바퀴다. 이를 2바퀴 실행한다. 삭제 OFF 조건은 간격에 따라 중복 실행하지 않는다. 해당 조건의 명령/파일명에는 공통 실행기의 기본 scan/check 값 1000이 남을 수 있지만 삭제 데몬은 시작하지 않으며 attempts 기록의 interval_ms는 null이다.

## 공통 동작과 주의사항

- 매 시도에서 .11의 /data4에 fstrim을 수행하고, 명령 성공 후 600초 기다린다. 재시도도 같은 준비를 다시 수행한다.
- PRECREATE: 필요한 시작 전 정리·검사 → fstrim → 600초 대기 → 토픽 사전 생성 및 준비 메시지 → 생산 → cleanup.
- AUTO: 시작 전 검사 → fstrim → 600초 대기 → 선택적 삭제 데몬 시작 → 생산(자동 토픽 생성) → cleanup.
- 활성 단계 사이의 10초 대기는 600초 fstrim 대기와 별도다. fio는 실행하지 않는다.
- PRECREATE에서도 마지막 cleanup은 ON이다. 실시간 비활성 삭제만 OFF다.
- 생산은 차량당 100Hz, 4096바이트, 체류 40초다. 비활성 삭제 기준은 600000ms다.
- build/drop-cache/iostat OFF는 이전 suite 조건을 유지한다. fstrim에는 기존 원격 접속 및 sudo 권한이 필요하다.
- 실패한 하위 실행과 복구가 끝나면 60초 대기 후 1회 재시도하고, 다시 실패하면 다음 조건으로 진행한다. 사용자 중단에는 재시도하지 않는다.
- 시도마다 고유 접두어로 충돌을 방지한다. 그러나 cleanup 실패의 잔여 상태는 후속 결과에 영향을 줄 수 있다. 기존 cleanup Java 예외 처리는 수정하지 않았다.
- 모든 산출물은 artifacts/, 전체 종료 후 생산 CSV 복사본은 metrics/에 저장한다. manifest.json에서 실패/부분 결과를 구분한다.
- PRECREATE는 현재 구현대로 TRIM 이후 생성 및 준비 쓰기를 수행한다. 따라서 생산 직전 SSD 상태가 AUTO와 완전히 같다고 가정하면 안 된다.

## 예상 시간

한 바퀴 6조건 × 5Hz = 30회, 두 바퀴 총 60회다. 재시도 없이 생산 설정 60시간 + fstrim 이후 대기 10시간 = 70시간이다. 실제로는 토픽 사전 생성, fstrim 명령 자체, 마지막 차량 종료, 단계 간 대기, cleanup 및 실패 재시도 시간이 더해진다. 모든 정규 회차에서 한 번씩 추가 시도하면 최대 120시도이며 같은 산식의 기본 시간은 140시간이고 추가 처리 시간은 별도다. 조기 실패한 시도는 전체 생산 시간을 채우지 않으므로 실제 실패 시나리오 시간은 달라진다.

## 로컬 검증

10개 모의/파일 테스트를 통과했다. 60개 조합의 순서, PRECREATE에서 삭제 OFF, OFF 조건의 중복 제외, 각 실행/재시도의 fstrim 전달, 600초 대기 및 단계 간 10초 전달을 확인했다. 실제 SSH/fstrim/Kafka 실험은 실행하지 않았다.
