# 순회 실행·결과 정리 업데이트 (로컬 준비본)

서버에는 반영하지 않았다. 실행 중인 실험도 변경하지 않았다.

## 실행 진입점

새 `run_client42_suite.sh`는 기존 비교 스크립트 대신 전체 순회를 관리한다. 기존 `run_remote_kafka_experiments.sh` 및 통합 실행기를 사용하므로 생산·삭제는 .42, fio 등 장치 작업은 기존 원격 경로를 따른다. 기존 comparison 진입점은 호환성을 위해 유지되며 새 자동 재시도·전체 수집은 suite에서 제공한다.

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  bash ./run_client42_suite.sh \
    --hz-list 3 4 5 \
    --rounds 2 \
    --delete-modes BATCH IMMEDIATE \
    --intervals 1000 300000 \
    --fio-passes 0 \
    --run-duration-minutes 60 \
    --failure-retries 1 \
    --retry-delay-seconds 60 \
    --step-delay-seconds 10 \
    --inter-experiment-delay-seconds 60 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results
)
```

전체 12개 조합을 한 바퀴 돈 뒤 두 번째 바퀴를 실행한다. 정규 실행 24회이며, 각 실패에 최대 1회 추가 시도가 있어 최악에는 48회까지 실행된다. `--rounds`는 계획된 전체 반복, `--failure-retries`는 실패 시 추가 실행이다. `--retry`와 혼동하지 않도록 별도 이름을 사용한다. 추가 시도도 완전한 60분 실험을 다시 실행하며 부분 이어받기가 아니다.

AUTO, delete/cleanup/stats ON, build/cache-drop/iostat/fstrim OFF를 현재 시나리오 기본으로 유지한다. fio 0은 비활성화이며 1 이상이면 그 횟수로 활성화한다. 새로운 시나리오를 지원하는 범용 옵션으로 오해하지 않도록 suite는 이 고정 실험 조건에 맞췄다.

## 단순한 결과 구조

```text
suite_시각_PID_KST/
  settings.json          적용한 suite 설정
  commands.jsonl         실행 전 명령 및 시도 식별자
  attempts.jsonl         각 시도의 시작·종료·종료 코드
  suite.log              KST 진행 기록
  artifacts/             모든 회차의 CSV, 상세 로그, 실행 인자 기록
  metrics/               전체 종료 시 복사한 생산 CSV와 manifest.json
  .runtime/              재사용하는 Java 보조 클래스 (분석 결과 아님)
```

Hz/삭제/간격/회차별 하위 폴더를 만들지 않는다. 기존 산출물은 옮기거나 삭제하지 않는다. 통합 실행기는 기본 `--output-layout flat`으로 동작하고, 기존 중첩 구조가 필요하면 `--output-layout nested`를 지정할 수 있다.

생산 파일명 예시(실제 실행 ID와 시각은 자동 결정):

`r1_c1_try2_3Hz_auto_fio0_del-on-batch_scan1000_check1000_60min_repeat1_20261008_180000_1234_5678_start_20261008_180001_KST_end_20261008_190120_KST_metrics.csv`

`r`=전체 순회, `c`=조합 순번, `try1`=최초 시도, `try2`=실패 후 재시도. `repeat1`은 하위 실행기의 단일 실행 설정이다. fio 횟수, 삭제 모드, scan/check 각각의 ms, 시간, 시작·종료 KST를 읽을 수 있다. 나머지 모든 인자는 같은 이름의 `.run.txt`와 commands.jsonl에서 추적한다. 시작 전 원격 검사 실패 등으로 회차 파일이 생성되지 않은 경우도 commands/attempts 기록에 남는다.

## 실패 및 수집 정책

- 실패하면 기존 통합 실행기의 복구 처리가 종료된 후 60초 기다리고 동일 조건을 1회 다시 실행한다.
- 두 번째도 실패하면 기록 후 다음 조합으로 진행한다. 최종 실패가 하나라도 남으면 모든 조합을 실행한 뒤 exit 1이다. 재시도에 성공한 조합은 최종 실패 수에서 제외하되 첫 실패 기록은 보존한다.
- 시도마다 고유한 토픽 접두어를 사용한다. 실패 잔여 토픽과 이름이 충돌하지 않지만 잔여 디스크·메모리 부하는 없어지지 않는다. cleanup 실패 후 이어진 측정은 이 영향을 별도로 평가해야 한다. 기존 cleanup Java 예외 처리 자체는 이번 변경에 포함하지 않았다.
- Ctrl+C/TERM/HUP은 재시도하지 않고 자식 종료를 기다린 뒤 종료한다. SIGKILL, 전원 종료처럼 처리할 수 없는 중단에서는 자동 수집이 보장되지 않는다.
- 전체 종료 또는 처리 가능한 중단 시 `*_metric.csv`, `*_metrics.csv`를 `metrics/`로 복사한다. 원본과 실패 시도의 CSV도 보존한다. deletion/stats 등 다른 CSV는 생산 CSV 수집에서 제외한다.
- `metrics/manifest.json`에는 원본·설정 파일 경로, 종료 코드, SHA-256이 있다. 종료 코드가 0이 아니거나 unknown인 CSV를 정상 회차와 섞어 분석하지 않는다. 종료 코드 0 역시 목표 생산율 달성의 증명은 아니다.
- 수집은 같은 내용의 기존 파일은 유지하고, 다른 내용의 동명 파일을 덮어쓰지 않는다.
- 다른 실험과 동시에 실행하지 않는다. suite와 기존 comparison은 동일 잠금을 사용하지만 다른 직접 진입점까지 강제로 막지는 않는다.

## 나중에 배포할 파일

1. `run_kafka_experiments.sh` → 프로젝트 루트 (수정)
2. `run_client42_suite.sh` → 프로젝트 루트 (신규)
3. `tools/run_experiment_suite.py` → tools/ (신규)

세 파일을 함께 .42의 `/home/hss544/kafka-test-clients/`에 반영한다. .11의 동기화는 실험 종료 후 별도로 수행한다. 기존 서버 버전과 차이를 확인하고 대상 파일을 백업한 후 반영해야 한다. JAR 또는 Java 코드는 바뀌지 않아 이번 변경만으로 JAR 재빌드는 필요하지 않다. 기존 서버의 remote helper, finalize_experiment.py 등 의존 파일은 그대로 필요하다.

배포 후 실험을 시작하기 전에 `bash ./run_client42_suite.sh --dry-run`으로 24개 실행 계획을 확인할 수 있다. dry-run은 서버 접속/생산/fio를 수행하지 않으며 결과 계획 파일만 로컬에 만든다.

## 로컬 검증

`tests/test_experiment_suite.py`: 전체 순회 순서, 실패 후 1회 재시도 및 다음 조합 진행, 중단 시 재시도 금지, 재시도 비활성화, 성공/실패 CSV 수집 및 동명 파일 덮어쓰기 방지를 모의 검사한다. 실제 Kafka/SSH/fio는 사용하지 않는다.

추가로 실제 finalize 도구를 임시 파일에 실행하여 flat 경로의 파일명 확정, 메타데이터 및 인덱스 연결을 검사했다. 새 기능 6개 모의/파일 테스트와 Bash 문법 검사를 통과했다. 기존 전체 Bash 회귀 테스트는 Windows Git Bash에서 `[STEP 5 대기]` 출력 존재 검사에 실패했다. 정리 시간이 대기 구간을 소진하면 해당 출력이 없는 시간 의존 검사이며, 전체 회귀 통과 또는 Linux 실서버 검증 완료로 간주하지 않는다. 배포 전 서버 실험 종료 후 해당 검증을 별도로 수행해야 한다.
