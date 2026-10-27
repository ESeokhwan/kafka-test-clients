# Kafka 실험 실행기

`run_kafka_experiments.sh`는 Linux에서 실험 준비, 생산, 통계 수집과 정리를 수행한다.

| `--topic-mode` | 생산 클래스 | 준비·정리 |
| --- | --- | --- |
| `auto` (기본값) | `AutoTopicProducerMetrics` | 토픽 충돌 검사, 생산 후 일반 토픽 cleanup |
| `precreate` | `AutoTopicProducerMetrics` | 사전 cleanup·토픽 생성·준비 전송, 생산 후 일반 토픽 cleanup |
| `transient` | `AutoTransientTopicProducerMetrics` | 워커에서 UUID 기반 생성, 선택적으로 Transient reset |
| `transient-precreate` | `AutoTransientTopicProducerMetrics --precreate` | 전체 사전 생성, 전체 생산 완료 후 전체 seal, 선택적으로 Transient reset |

## Monitoring 로그 rollout 앱

`BrokerExtension` 앱은 클라이언트 호스트에서 지정한 **브로커 한 대**에 extension 요청을 보내고
응답을 기다린다. 커스텀 `kafka-clients`를 포함해 `:apps:shadowJar`로 빌드하면 실행 환경에서
`kafka-tt` 소스나 `kafka-broker-extension.sh`의 위치를 알 필요가 없다.
빌드 환경의 Kafka 소스 위치는 `local.settings.gradle`의 `CUSTOM_KAFKA_PATH`로 지정한다.

```bash
bash scripts/kafka_clients_run.sh apps BrokerExtension \
  --broker localhost:9092 \
  --target monitor-log-rollout \
  --operation flush-and-rollout \
  --timeout-ms 30000
```

성공 시 `0`, 요청 오류·연결 실패·시간 초과 시 `1`, 인수 오류 시 `2`, Java 대기 중
인터럽트 시 `130`으로 종료한다. 별도 결과 파일은 만들지 않으며, 호출하는 스크립트는
종료 코드로 성공 여부를 확인하면 된다. `--help`는 브로커에 연결하지 않는다.

`-b`, `--bootstrap-server`는 `--broker`의 별칭이다. 쉼표로 구분한 여러 주소는 받지 않는다.
여러 브로커의 로그를 전환하려면 각 주소에 따로 호출하고 모두 성공했는지 확인해야 한다.
`--command-config FILE`로 SSL/SASL 등 Kafka client properties를 읽으며 명시한 브로커와
timeout이 파일의 `bootstrap.servers`, `request.timeout.ms`보다 우선한다.
`--timeout-ms`는 클라이언트 설정 후 연결·인증·응답 대기까지 적용되며 브로커의 허용 timeout
(기본 30000ms) 이하여야 한다. 시간 초과가 브로커에서 이미 시작한 작업의 취소를 뜻하지는 않는다.
요청은 자동 재시도하거나 다른 브로커로 전환하지 않는다.

`--target`의 기본값은 `monitor-log`이며 `--operation flush`로 파일 전환 없이 flush할 수도 있다.
다른 extension은 `--target`, `--operation`, 선택적 UTF-8 `--payload`, `--payload-version`으로 호출한다.
`--request-id`에는 Kafka UUID의 URL-safe Base64 표현을 지정할 수 있으며, 생략하면 새 ID를 생성해
시작 로그에 표시한다. 같은 ID를 다시 보냈을 때의 중복 처리 방식은 브로커 handler에 따른다.

브로커는 `broker.extension.request.enabled=true`와 접속 listener에 대한
`broker.extension.request.allowed.listeners` 설정이 필요하다. Authorizer를 사용하면 클러스터
`ALTER` 권한도 필요하다. Rollout은 monitoring writer 큐의 경계까지만 처리하므로 실험 종료 시
생산기·삭제기의 종료를 먼저 기다려야 한다.

`run_kafka_experiments.sh`는 매 실험 회차의 종료 처리에서 `BrokerExtension`을 자동 호출한다.
정상 종료 시 cleanup/reset과 회차 간 대기까지 마친 뒤 호출하며, 실패·중단 시에도 기존 복구/종료
처리가 끝난 뒤 시도한다. `--brokers`의 쉼표로 구분된 주소마다 한 번씩 요청하며, 단일·마지막
회차에도 적용한다. 단독 `fio`, `delete`, `cleanup` 모드에는 적용하지 않는다.

각 요청의 명령, stdout/stderr, 성공 여부와 종료 코드를 같은 회차의 `.run.txt`에 남긴다.
실패는 `stage=MONITOR_ROLLOUT_FAILED ... exit_code=... continue=true`로 기록하며 자동 재시도 없이
다른 브로커와 다음 실험을 계속한다. Rollout 실패는 원래 실험의 성공/실패 판정이나 복구 정책을
바꾸지 않는다. 이전 apps JAR에 앱이 없는 경우에도 실행 오류를 기록하고 계속한다.

실행기의 `--monitor-rollout-timeout-ms`(기본 30000)는 각 요청의 제한 시간이며,
인증 설정이 필요하면 `--monitor-rollout-command-config FILE`을 지정한다.
`MONITOR_ROLLOUT_TIMEOUT_MS`, `MONITOR_ROLLOUT_COMMAND_CONFIG` 환경 변수로도 설정할 수 있다.
`--broker-ssh`를 사용해도 rollout 요청은 클라이언트 호스트에서 Kafka API로 전송한다.

## Transient 생산

```bash
bash run_kafka_experiments.sh --topic-mode transient --hz 3
```

`--all`, `--retry`, 생산 수치, fio, TRIM, 캐시 비우기, iostat와 결과 파일 관리 옵션은 공통으로 사용한다.
fio·TRIM·캐시 비우기는 실험에서 기본 ON이다. 실행기는 두 Transient 모드에서 일반 토픽의 충돌 검사·
사전 생성과 JMX 삭제기·삭제 데몬 컴파일을 생략한다. `--delete`, `--ignore-response`와
일반 토픽 준비 옵션은 사용할 수 없다. Transient Producer는 `acks=all`을 요구한다.

apps JAR에는 이 생산 클래스와 커스텀 Kafka의 Transient API가 포함되어야 한다.
빌드와 기존 JAR 재사용 모두에서 필요한 클래스의 존재를 확인한다.

Transient 토픽을 모두 생성한 다음 생산하려면 `transient-precreate` 모드를 사용한다.

```bash
bash run_kafka_experiments.sh --topic-mode transient-precreate --hz 3 \
  --topic-prepare-timeout-seconds 600
```

`--topic-cnt`로 지정한 수(미지정 시 Hz와 실행 시간으로 계산)의 모든 생성 응답을 기다린 뒤,
기존 Hz 간격으로 생산 워커를 시작한다. 준비 시간은 각 워커의 `--topic-lifetime-ms`에 포함하지 않는다.
생성 실패나 준비 시간 초과가 발생하면 메시지 생산을 시작하지 않는다. 전체 준비 제한 시간은
`--topic-prepare-timeout-seconds`로 지정하며 기본값은 300초다. 실행기가 생산기에 `--precreate`와
준비 제한 시간을 전달한다. 선택적 fio 이후에 생산기 내부에서 전체 생성 → 생산을 수행한다.
이 모드에서는 개별 워커 종료 시 seal하지 않고, 모든 워커의 생산·flush·종료가 끝난 뒤 전체 토픽을
seal하고 응답을 기다린다. 이후 cleanup이 켜져 있으면 기존 Transient 모드와 같이 reset한다.
일반 토픽용 `--topic-mode precreate`는 별도의 모드다.

## Transient reset

cleanup은 기본 ON이며, 생산 종료 후 `apps ResetTransientTopics`를 실행한다.
이 앱은 `AdminClient.resetTransientTopics()`의 완료를 기다린 뒤 성공 시 0,
오류·시간 초과 시 1, 대기 중 인터럽트 시 130으로 종료한다. `--no-cleanup`으로 reset을 생략할 수 있다.

앱을 직접 사용하는 명령은 다음과 같다.

```bash
bash scripts/kafka_clients_run.sh apps ResetTransientTopics \
  --bootstrap-server localhost:9092 --timeout-ms 300000
```

`-b`, `--brokers`도 브로커 옵션으로 사용할 수 있다. `--command-config FILE`로 Admin/security
properties를 읽을 수 있으며, 명시한 브로커 인수가 파일의 `bootstrap.servers`보다 우선한다.
`--help`는 Kafka에 연결하지 않는다.

기존 외부 reset 스크립트를 사용하려면 기본 앱 대신 다음 옵션으로 지정한다.

```bash
bash run_kafka_experiments.sh --topic-mode transient --hz 3 \
  --cleanup --transient-reset-script /path/to/reset-transient-topic.sh
```

실행기는 **클라이언트 호스트**에서 앱 또는 외부 스크립트를 실행한다. `--broker-ssh`를 사용해도
클라이언트에서 Kafka API로 접속하는 형태다. 외부 스크립트의 인수는 다음과 같다.

```text
bash SCRIPT --bootstrap-server BROKERS --timeout-ms MILLISECONDS
```

`MILLISECONDS`는 `--cleanup-timeout-seconds`(기본 300초, 최대 86400초)를 변환한 값이다.
스크립트는 reset 완료를 기다리고 성공 시 0, 실패 시 0 이외의 종료 코드를 반환해야 한다.
실행기에도 같은 시간 제한이 적용된다.

기존 JAR을 재사용할 때도 기본 reset 앱과 커스텀 reset API 클래스의 존재를 생산 전에 확인한다.

reset은 PREFIX 범위 삭제가 아니라 **클러스터 전체 Transient 데이터·인덱스 초기화**다.
모든 Transient 클라이언트를 종료하고 하나의 reset만 실행하는 실험 환경을 전제로 한다.
reset 성공은 새 내부 토픽·코디네이터의 준비 완료를 뜻하며, 이전 로그 파일의 물리적 삭제 완료까지
보장하지는 않는다.

생산 프로세스 종료 후 reset하며, 단일·마지막 회차에도 적용한다. reset 시간은 회차 간 대기에 포함된다.
생산 실패 시에도 프로세스를 종료한 뒤 reset에 성공해야 다음 회차로 진행한다.
reset 실패·시간 초과는 자동 재시도하지 않고 전체 실행을 중단한다.
`--no-cleanup`인 경우 실패 복구 reset도 생략하고 전체 실행을 중단한다.

결과 파일명과 `.run.txt`에 `TOPIC_MODE`, `PRODUCER_CLASS`, `CLEANUP_POLICY`를 기록한다.
reset 명령과 결과는 같은 회차의 `.run.txt` 및 `_reset.log`에서 확인할 수 있다.
