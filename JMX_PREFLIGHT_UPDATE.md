# JMX/RMI 연결 수정 및 사전 검사 (2026-10-06)

.11의 `/data4/kafka-tt/config/server.properties`에 사용자 정의 `jmx.rmi.hostname=166.104.110.11`을 추가했다. Kafka의 기본 설정 항목이 아니므로 `bin/kafka-server-start.sh`가 이를 읽어 `KAFKA_OPTS`에 `-Djava.rmi.server.hostname=166.104.110.11`을 전달한다. 기존 포트·인증·SSL·힙 설정은 유지한다. 다른 시작 경로를 사용하면 해당 JVM 옵션을 직접 전달해야 한다.

사용자 요청으로 브로커 재시작은 보류했다. 따라서 실행 중인 JVM에는 수정이 적용되지 않았고, .42 → .11 JMX 연결 성공은 재시작 후 검증해야 한다. 브로커 JAR 재빌드는 필요 없다.

실험 실행기는 삭제 ON인 각 회차와 단독 delete 실행에서 registry 연결, 후속 RMI 연결, Kafka MBean 조회를 검사한다. 도구는 `tools/JmxPreflight.java`이며 삭제기와 함께 javac로 컴파일하므로 생산기 JAR 재빌드도 필요 없다. 삭제 OFF이면 검사를 생략한다.

회차에서는 cache drop·시작 전 cleanup·토픽 검사/생성·TRIM·fio·생산보다 먼저 검사한다. 한도는 기존 `--delete-start-timeout-seconds`(기본 60초)를 사용한다. 실패/시간 초과 시 기존 토픽을 건드리지 않고 전체 실행을 중단한다. `_jmx_preflight.log`에 상세 오류, `.run.txt`에 실제 검사 명령·단계·성공/실패 원인을 남긴다. 진행 표시는 1초 간격 KST 로그다.

.11 원본 백업: `/data4/kafka-test-clients/.codex-backups/broker-jmx-20261006_103918.tar.gz` 및 `jmx-preflight-20261006_103918.tar.gz`.

서버 작업은 각 서버의 codex tmux에서 수행하며 검증/배포 기록은 프로젝트 `codex_logs/deploy-jmx-preflight-20261006.log`, 실제 연결 검사 기록은 .42의 `codex_logs/verify-jmx-probe-20261006.log`에 남긴다.
