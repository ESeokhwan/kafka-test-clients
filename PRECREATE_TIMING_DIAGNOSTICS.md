# PRECREATE 지연 검증 코드

작성: 2026-10-20. 로컬 구현·검증만 수행했으며 서버 배포, 서버 빌드, 실제 Kafka 실험은 수행하지 않았다.

## 목적

PRECREATE와 AUTO 모두 차량 유입 시 워커와 KafkaProducer를 새로 만들고, 차량별 생산 시간이 지나면 flush/close 후 종료한다. 이 생명주기를 변경하지 않고 다음을 구분한다.

1. 워커/Producer 초기화가 늦는가?
2. 송신 예정 시각에 비해 실제 send 진입이 늦는가?
3. send 내부에서 오래 기다리는가, 반환 후 ACK까지 오래 기다리는가?
4. 첫 메시지에 국한된 지연인가, 생애 후반에도 지속되는가?
5. 여러 차량의 10ms 송신 시점이 몰리며, 이를 분산하면 지연이 줄어드는가?

## 파일 변경

- `kafka-test-clients_extracted/kafka-test-clients/apps/src/main/java/org/example/apps/AutoTopicProducerMetrics.java`: 기존 생산 경로에 선택적 계측과 최초 송신 지연 추가. 기존 일반 metrics 통계 분류와 async 방식은 유지.
- 같은 디렉터리의 `ProducerTimingProbe.java`: 고정 크기 히스토그램, 송신 시점 분포, 순서 독립적인 send/ACK 집계.
- `run_client42_precreate_timing_check.sh`: .42→.11 대조 실험 실행기. 실험 자동 시작 없이 파일만 추가.
- `tests/ProducerTimingProbeTest.java`: 실제 Kafka 접속 없이 집계와 동시성·재현성 검증.

기존 파일의 “사전 생성 시 데이터를 보내지 않는다”는 주석은 실제 seed 1건 동작에 맞게 바로잡았다. `--help`도 추가하여 실제 준비 작업 전에 새 JAR의 옵션 지원 여부를 확인할 수 있게 했다.

## 옵션

| 옵션 | 기본 | 동작 |
| --- | --- | --- |
| `--timing-diagnostics` | OFF | 별도 진단 CSV 작성 |
| `--send-phase-jitter-ms 10` | 0 | 차량별 최초 생산 시작을 [0,10)ms 범위에서 한 번 지연 |
| `--send-phase-seed 42` | 42 | 토픽 이름과 조합해 재현 가능한 지연 선택 |

통합 실행기에 전달할 때는 마지막 `--` 뒤에 둔다. 통합 실행기의 기존 추가 Java 인수 전달 기능을 사용하므로 쉘 옵션 파서 자체는 변경하지 않았다. 실제 전달 인수는 기존 `.run.txt`의 생산 명령에 남는다.

```bash
# 기존 실험 명령의 맨 끝에 추가: 계측만 켜기
-- --timing-diagnostics --send-phase-jitter-ms 0 --send-phase-seed 42
```

```bash
# 기존 실험 명령의 맨 끝에 추가: 계측 + 최초 송신 시점 분산
-- --timing-diagnostics --send-phase-jitter-ms 10 --send-phase-seed 42
```

지연 범위는 한 생산 주기 이하만 허용한다. 100Hz에서는 최대10ms다. NaN/무한대/음수는 거부한다. ACK 진단과 `--ignore-response`의 동시 사용은 금지한다.

**워커 시작 스케줄은 그대로이며, jitter 대기 후 차량의 생산 수명 시계를 시작한다.** 따라서 40초 중 일부를 일부러 기다리면서 생산량을 줄이는 방식이 아니다. 대신 최초 데이터 생산 시각은 유입 대비 최대10ms 미뤄진다. 동일 설정·seed는 같은 요청 지연을 만들지만 OS 스케줄링까지 같게 만들지는 않는다. 마지막 워커 종료 시각도 그만큼 달라질 수 있다.

## 진단 산출물

기존 `..._metrics.csv`와 별도로 `..._metrics_timing.csv`가 생성된다. 기존 회차 파일명 확정기는 같은 실행 태그를 가진 파일을 함께 이름 변경한다. 다만 기존 다운로드 도구가 정해진 파일 종류만 가져오는 경우 이 신규 진단 파일은 별도 다운로드 대상에 추가해야 한다. 이번에는 다운로드 도구나 서버 배포 경로를 변경하지 않았다.

토픽마다 3개 그룹 × 4개 측정항목 = 12행이다.

### 그룹

- `first`: 첫 메시지. PRECREATE/AUTO 모두 동일한 기준.
- `after_first_under_1s`: 첫 메시지를 제외하고 생산 시작 후 1초 미만에 send에 진입한 메시지.
- `from_1s`: 1초 이상에 진입한 메시지.

기존 metrics CSV의 첫 메시지 포함 규칙은 호환성을 위해 유지했다. **공정한 첫 메시지 제외 비교는 신규 파일의 후자 두 그룹을 이용한다.** 그룹 mean을 단순 평균하지 말고 해당 측정항목 Count로 가중한다. PRECREATE의 첫 메시지가 일반 통계에 들어가는 문제를 기존 결과와 섞어서 몰래 변경하지 않는다.

### 측정항목

| Measure | 의미 |
| --- | --- |
| `schedule_lag` | 예정 송신 시각 → send 계측 진입. 메시지 생성과 워커 스케줄링 지연도 포함 |
| `send_call` | send 계측 진입 → send 반환. metadata/큐 대기 등 포함 가능 |
| `send_to_ack` | send 계측 진입 → 성공 ACK 콜백 진입 |
| `post_return_to_ack` | send 반환 → 성공 ACK 콜백 진입. 콜백이 먼저 오면 0 |

모두 `System.nanoTime()` 기반 차이를 사용하며 단위는 **µs**다. send_to_ack와 post_return_to_ack에는 실패 응답을 포함하지 않는다. send_call은 성공·실패 완료 양쪽을 포함한다. send 반환 전에 콜백이 실행될 수도 있으므로 `send_call + post_return_to_ack == send_to_ack`가 항상 성립하는 것은 아니다.

`send_call`의 시작점은 계측 표본 생성 직전이므로 집계 진입 및 콜백 객체 구성의 작은 비용도 포함한다. Kafka 내부 처리만의 순수 시간으로 해석하지 않는다. 기본 metrics의 응답 시각은 진단 집계를 수행하기 전에 확보하지만, 진단이 Sender 스레드에서 소비하는 CPU 시간은 이후 메시지에 영향을 줄 수 있다.

- `Count`, `MeanUs`, `MaxUs`: 해당 측정항목의 표본 수, 평균, 최댓값.
- `P90UpperUs`, `P99UpperUs`: **100µs 폭 히스토그램의 상한값**, 정밀한 원시 표본 분위수가 아니다. 100ms를 초과하는 분위수는 `>100000`으로 표시한다.
- `OverflowAbove100ms`: 해당 측정항목에서100ms 초과 횟수. 평균과 max에는 실제 값을 반영한다.
- `Attempted/Completed/Failed`: 그룹별 계측 시도, 반환과 콜백 양쪽을 확보한 완료, 실패 수. 여러 Measure 행에 반복된 값이므로 네 번 합산하면 안 된다.
- `AckBeforeReturn`: ACK가 send 반환 이전에 발생한 성공 표본 수.
- `ScheduleResets`: 기존 생산 루프가 지연 때문에 다음 송신 일정을 재설정한 횟수. 토픽 단위 반복 열이다.
- `WorkerStartOffsetUs`: 본 실행의 공통 단조 시계 기준점에 대한 워커 진입 시각.
- `ProducerInitUs`: KafkaProducer 생성 호출 시간. 연결이 모두 준비된 시간이라는 뜻은 아니다.
- `ProductionStartOffsetUs`: jitter 이후 생산 수명 시작 시각.
- `WorkerEndOffsetUs`: flush/close가 끝나고 진단 결과를 기록하는 시각. 실제 스레드 종료 직전의 근사 시각이다.
- `PhaseDelayUs`: 선택한 초기 지연. 실제 sleep의 정확한 소요시간을 뜻하지 않는다.
- `PhaseBinCounts`: 공통 기준점 대비 송신 주기 내 위치를100구간으로 나눈 횟수. `schedule_lag` 행에만 세미콜론으로 저장한다. 100Hz에서는 한 구간100µs. 토픽별·그룹별 합계를 이용해 송신 집중도를 비교한다. 실행마다 기준점이 달라 빈 번호 자체보다 분포의 집중도/모양을 비교한다.

파일은 토픽 종료 시 집계 결과만 기록한다. 매 메시지 로그 쓰기나 ACK 대기를 추가하지 않는다. 단, 계측용 객체·동기화·히스토그램 갱신 비용은 있으므로 계측 OFF 대조군이 필요하다. 강제 종료·Producer 생성 실패·flush/close 실패 시 일부 토픽 행이 없을 수 있다. 성공 종료 상태, 토픽 수, 그룹별 Attempted/Completed를 함께 확인하고 부분 파일을 완료 데이터로 취급하지 않는다. 이 CSV만으로 네트워크/브로커 내부 구간을 더 세분하거나 순간적인 fsync를 직접 식별할 수는 없다.

## 대조 실험 실행

새 Java 소스로 JAR를 빌드하고 .42에 배포한 **이후** 아래처럼 실행한다. 현재 제공 스크립트는 `--no-build`로 동작하며 실행 전 JAR 도움말에서 새 옵션 지원을 확인한다. 기존 서버의 JAR를 이번 작업에서 바꾸지는 않았다.

```bash
cd /home/hss544/kafka-test-clients

# 실제 준비나 생산 없이 4Hz, 3회, 각60분 명령만 확인
bash ./run_client42_precreate_timing_check.sh --dry-run 4 3 60

# 검토 및 배포 완료 후에만 수동 실행
bash ./run_client42_precreate_timing_check.sh 4 3 60
```

각 회차의 조건은 PRECREATE/AUTO 각각 다음 세 가지다.

1. 계측 OFF·jitter0: 기존 경로의 대조군.
2. 계측 ON·jitter0: 계측 영향과 본래 지연 위치 확인.
3. 계측 ON·jitter10ms: 송신 집중 가설 검증.

총6조건×3회=18실험이다. 생산60분+fstrim대기10분만으로 최소21시간이며, 사전 생성·cleanup·마지막 차량·단계 대기는 추가된다. 홀수 회차는 위 순서, 짝수 회차는 역순으로 실행한다. 순서 효과를 줄이지만 완전한 무작위 블록 설계는 아니다.

공통 설정은 fio OFF, 삭제 OFF, cleanup ON, iostat ON, drop-cache OFF, fstrim ON+600초, 단계10초다. 테스트 전용 접두어 `timing_probe`를 쓰며 현재 일반 실험의 `test_creator`와 구분한다. 이 접두어도 동시 실행 간 공유되므로 **다른 실험과 동시에 실행하지 않는다**. 실패하면 상위 스크립트를 중단하고 다음 조건을 시작하지 않는다.

## 해석 기준

| 결과 | 지지되는 설명 / 후속 확인 |
| --- | --- |
| OFF와 ON 결과 차이가 큼 | 계측 영향이 무시 불가능. jitter 효과보다 먼저 계측 비용을 줄일 것 |
| first만 느리고 from_1s는 유사 | 초기 연결/metadata 및 첫 메시지 모집단 효과 |
| from_1s의 send_call 증가 | 클라이언트 동기 경로 지연 후보. metadata/버퍼/CPU 추가 계측 필요 |
| from_1s의 post_return_to_ack 증가 | 비동기 전송·네트워크·브로커·콜백 스케줄링 경로 후보 |
| schedule_lag 증가 | 워커/메시지 생성/스케줄링이 생산 일정을 지키지 못함 |
| jitter에서 phase 집중과 mean이 함께 감소 | 송신 집중 가설을 지지. 부하 달성률이 함께 떨어져서 개선된 것은 아닌지 확인 |
| phase가 분산되어도 지연 유지 | 송신 위상만으로 설명하기 어려움. 전체 토픽 수·seed·서버 상태 대조로 이동 |

latency 개선만 보지 말고 생산량, Missing Response Count, 진단 완료 수, iostat를 함께 비교한다. PRECREATE seed/토픽 수를 바꾸는 대조는 이번 코드에는 포함하지 않았다. 한 번에 여러 실험 변수를 바꾸지 않기 위해서다.

## 로컬 검증

기존 의존성 JAR를 classpath로 Java17 타깃 컴파일을 수행했다. 실제 배포용 fat JAR 재빌드는 아직 하지 않았다.

- 신규 집계: 정상 순서, ACK 선행, 중복 통지, 실패 제외, 1초 경계,100ms overflow, 동시 반환/ACK, jitter 범위/재현성.
- 기존 회귀: CallbackDiagnosticsTest, PrecreateSeedTest, TopicMetadataRetryTest, VehicleMessageGeneratorTest.
- 실행기: Bash 구문 및 dry-run. 실제 브로커 접속·fstrim·Kafka 생산은 실행하지 않았다.
