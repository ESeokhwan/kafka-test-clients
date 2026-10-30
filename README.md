# Kafka 실험 실행기

`run_kafka_experiments.sh`는 Linux에서 실험 준비, 생산, 통계 수집과 정리를 수행한다.

| `--topic-mode` | 생산 클래스 | 준비·정리 |
| --- | --- | --- |
| `auto` (기본값) | `AutoTopicProducerMetrics` | 토픽 충돌 검사, 생산 후 일반 토픽 cleanup |
| `precreate` | `AutoTopicProducerMetrics` | 사전 cleanup·토픽 생성·준비 전송, 생산 후 일반 토픽 cleanup |
| `transient` | `AutoTransientTopicProducerMetrics` | 워커에서 UUID 기반 생성, 선택적으로 Transient reset |
| `transient-precreate` | `AutoTransientTopicProducerMetrics --precreate` | 전체 사전 생성, 전체 생산 완료 후 전체 seal, 선택적으로 Transient reset |

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

일반 Transient 모드는 첫 `send(topicName, ...)`에서 브로커의 자동 생성 기능을 사용한다.
각 워커에서 명시적으로 생성한 뒤 생산하려면 생산기 옵션 `--need-manual-create`를 전달한다.

```bash
bash run_kafka_experiments.sh --topic-mode transient --hz 3 \
  --need-manual-create --need-manual-seal
```

두 옵션은 실행기에서 직접 지정할 수 있으며 기본값은 모두 OFF다. `--no-manual-create`,
`--no-manual-seal` 또는 `--need-manual-create=false`, `--need-manual-seal=false`로 끌 수 있다.
`NEED_MANUAL_CREATE`, `NEED_MANUAL_SEAL` 환경 변수도 지원하며 CLI가 우선한다.
설정은 회차별 `.run.txt`와 실제 생산기 명령에 기록한다. 기존 `--` 뒤 전달 방식도 지원한다.
두 옵션은 `transient`, `transient-precreate` 실험에서만 켤 수 있다.

이 옵션은 모든 토픽을 한꺼번에 준비하지 않고, 기존 Hz 간격으로 시작한 각 워커에서
`producer.create(topicName)`의 응답을 기다린 뒤 반환된 UUID로 메시지를 전송한다.
생성 대기는 Producer의 `delivery.timeout.ms`로 제한한다. 생성 실패·시간 초과 시 해당 워커만
오류 로그를 남기고 종료하며, 다른 워커는 각자 생성·생산을 계속한다. 실패한 토픽은 자동 생성으로
전환하지 않는다. 개별 생성 실패를 전체 앱 오류로 전파하지 않고, 나머지 워커가 완료되면
생성된 토픽의 seal과 metrics 저장을 수행한다. 생성 시간은 `TopicCreateLatencyMs`에 기록하고
`--topic-lifetime-ms`에서는 제외한다. 첫 메시지부터 생산 latency 통계에 포함하며,
`--share-producer`, `--need-manual-seal`과 함께 사용할 수 있다.
`--precreate`와 함께 지정하면 사전 생성된 토픽을 사용하므로 중복 생성하지 않는다.

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
