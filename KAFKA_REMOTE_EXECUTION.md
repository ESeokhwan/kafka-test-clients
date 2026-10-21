# .42 클라이언트 / .11 브로커 분산 실행

진입점은 .42의 `/home/hss544/kafka-test-clients/run_remote_kafka_experiments.sh`입니다. `run_kafka_experiments.sh`에 `--broker-ssh hss544@166.104.110.11 --broker-project-dir /data4/kafka-test-clients --jmx-host 166.104.110.11`을 지정해도 같습니다.

| 작업 | 실제 실행 위치 |
| --- | --- |
| 실험 제어, 토픽 생성/준비 메시지, 본 생산, cleanup 요청, 삭제기 | .42 |
| Gradle 빌드, cache drop, fstrim, fio, iostat | .11 |
| CSV·통합 실험 로그·실행 인자 기록 | .42의 output-dir |
| 원격 작업별 요청·KST 로그·종료 코드·빌드 JAR 사본 | .11의 `/data4/kafka-test-clients/.remote_jobs/` |

## 연결 준비

.42의 hss544 계정에서 `.11`에 비대화식 SSH 접속이 가능해야 합니다. 개인키는 .42에 두고 공개키를 .11의 계정에 등록해야 하며 호스트 키도 신뢰 목록에 있어야 합니다. 인증 설정을 자동 변경하지 않습니다. 다음 읽기 전용 명령으로 확인합니다.

```bash
ssh -o BatchMode=yes -o StrictHostKeyChecking=yes hss544@166.104.110.11 true
```

특정 키는 `--remote-identity-file /home/hss544/.ssh/KEY`로 지정합니다. 암호화된 키는 사전에 .42의 ssh-agent에서 사용할 수 있게 준비하세요. SSH agent forwarding은 사용하지 않습니다. .11에서 `sudo -n`으로 cache drop 명령을 실행할 권한도 필요합니다. 전체 스크립트는 hss544 계정으로 실행하세요.

`.42 → .11:9092`와 Kafka advertised.listeners의 주소가 접근 가능해야 합니다. `--delete` 사용 시 JMX/RMI도 .42에서 접근 가능하도록 브로커가 광고하는 호스트와 포트가 설정되어야 합니다. precreate의 `--no-delete` 및 일회성 cleanup은 JMX 연결 없이 Kafka Admin API로 처리합니다.

## precreate / fio OFF / 1~5 Hz

`.42`의 `codex` 세션에 접속한 뒤 실행합니다. `--all`을 `--hz 1`~`--hz 5`로 바꾸면 개별 실행입니다.

```bash
(
  set -e
  cd /home/hss544/kafka-test-clients
  bash ./run_remote_kafka_experiments.sh \
    --build --all --retry 1 --topic-mode precreate \
    --no-fio --no-delete --cleanup --drop-caches --iostat --stats-enabled \
    --fstrim --fstrim-target-dir /data4 --fstrim-wait-seconds 600 \
    --brokers 166.104.110.11:9092 --prefix test_creator \
    --run-duration-minutes 20 --topic-cnt -1 \
    --topic-lifetime-ms 40000 --produce-hz-per-topic 100 --msg-size 4096 \
    --start-index 0 --start-barrier-delay-ms 10000 \
    --topic-prepare-timeout-seconds 600 --step-delay-seconds 10 \
    --inter-experiment-delay-seconds 60 --remote-stage-timeout-seconds 7200 \
    --output-dir /home/hss544/kafka-test-clients/experiment_results/remote_precreated_no_fio
)
```

fio ON에서는 `--no-fio`를 아래 옵션들로 교체합니다. 경로와 장치는 **.11 기준**입니다.

```text
--fio --fio-target-dir /data4 --fio-size-gib auto --fio-passes 3 --reserve-gib 2 --expected-disk nvme1n1
```

기존 문서의 .11 로컬 실행 블록을 .42에서 그대로 사용하지 마세요. .42에서는 위 원격 진입점과 `/home/hss544/kafka-test-clients` 경로를 사용합니다.

## 실행·실패 처리

- 각 Hz/반복에서 TRIM은 기본 ON입니다. 시작 전 cache drop → (precreate이면 기존 토픽 정리) → 토픽 충돌 검사 → **.11에서 fstrim → 최소 600초 대기** → (precreate이면 사전 생성·준비 메시지) → 선택적 fio → 본 생산 → cleanup → cache drop 순서입니다. fio 직후에는 TRIM하지 않습니다.
- `--fstrim-wait-seconds 1200`이면 20분 대기합니다. 600 미만은 오류이며 `--no-fstrim`은 TRIM과 대기를 모두 생략합니다. 단독 fio/delete/cleanup 모드에는 기본 적용하지 않습니다. 대기는 성공한 fstrim 종료 시점부터 별도로 계산하며 단계 간 10초/종료 cleanup 대기와 독립입니다.
- `.11`의 `--fstrim-target-dir`(기본 fio 대상 디렉터리)의 파일시스템 전체에 적용합니다. `--expected-disk`(기본 nvme1n1)가 일치해야 하며 비대화식 `sudo -n fstrim -v -- /data4` 권한이 필요합니다. TRIM 실패 시 해당 회차는 생산하지 않고 실패 기록·cleanup 정책을 따릅니다.
- 대기는 .42 제어기에서 1초마다 경과/남은 시간을 KST로 표시합니다. 요청 대기시간·실제 대기시간·TRIM 명령·결과가 회차 `.run.txt`와 `_fstrim.log`에 저장됩니다. TRIM 후 대기한다고 SSD 내부 GC가 완전히 종료되었다고 보장하지는 않습니다.

- .11 빌드 완료 후 실행별 JAR 사본을 .42에 다운로드하고 SHA256 일치를 확인한 뒤 로컬 JAR를 원자적으로 교체합니다. `--no-build`는 .42에 있는 기존 JAR를 사용하며 다운로드도 생략합니다.
- 토픽 생성·생산기는 .42에서 실행합니다. 삭제기 클래스도 .42에서 전달받은 fat JAR를 의존성으로 컴파일합니다. .42에 Kafka 전체 소스/빌드가 없어도 됩니다.
- cache drop은 회차 전후 기본 ON입니다. fio 및 cache 작업이 완료돼야 다음 단계로 진행합니다. 옵션의 원격 작업 제한은 build/cache/fio에 적용되고 iostat 수집의 상한은 7일입니다.
- 원격 명령은 .11의 codex tmux에서 작업별 창으로 실행하며 종료 시 창도 종료됩니다. 별도 상시 Python 로그 프로세스를 만들지 않습니다. build 상세 출력은 폐기하고 경과·종료 상태만 저장합니다. fio/iostat 원본 출력은 원격 작업 로그와 .42 단계 로그에 저장합니다.
- 클라이언트는 2초마다 생존 신호를 전달합니다. SSH 연결/클라이언트가 끊기면 원격 작업을 취소하며 최악의 경우 15초 신호 만료와 최대 5초 종료 대기가 적용됩니다. 단계 실패는 기존 실험 실패/cleanup 정책으로 전달됩니다.
- 원격 build/cache/fio 작업끼리는 잠금으로 동시 실행을 거부합니다. **동일 토픽 접두어를 쓰는 전체 실험을 동시에 실행하지 마세요.** 잠금은 전체 실험 기간을 보호하지 않습니다.
- 원격 로그의 RUN_ID와 .42의 단계 로그 안의 REMOTE_JOB 경로로 연결합니다. 원격 로그는 .remote_jobs에 보존되므로 장기 사용 시 별도로 보관/정리하세요.

현재 문서의 실험 명령은 실제 Kafka/fio 부하를 실행합니다. 배포 검증에서는 모의 빌드·모의 fio와 도움말/연결 검사만 사용합니다.
