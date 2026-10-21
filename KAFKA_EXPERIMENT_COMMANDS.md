# Kafka 실험 실행 문서

실행 방식별 문서를 분리했습니다.

- [1~5 Hz 순차 실행](KAFKA_EXPERIMENTS_SEQUENTIAL.md): 조건별 31개 명령과 전체 155회 자동 실행 블록.
- [개별 Hz 실행](KAFKA_EXPERIMENTS_SINGLE_HZ.md): 조건별·Hz별 155개 단독 실행 블록.
- [사전 생성 순차 실행](KAFKA_PRECREATED_SCENARIOS.md): 시작 전 cleanup → 사전 생성·토픽당 준비 메시지 1개 → 선택적 fio → 생산 → 종료 cleanup. fio ON/OFF 각각 1~5 Hz 순차 블록.
- [사전 생성 개별 Hz 실행](KAFKA_PRECREATED_SINGLE_HZ.md): fio ON/OFF 각각 1~5 Hz, 총 10개 독립 실행 블록.

명령 블록에는 빌드 ON(`--build`), 단계 간 10초(`--step-delay-seconds 10`), 캐시 비우기 ON(`--drop-caches`)를 명시했습니다. 기존 JAR 재사용은 `--build`를 `--no-build`로, 캐시 비우기 비활성화는 `--drop-caches`를 `--no-drop-caches`로 교체하세요. 토픽 준비 조회의 일시적 오류는 기존 준비 제한 시간 안에서 약 1초마다 자동 재시도합니다. `--retry`는 별도로 실험 총 반복 횟수입니다.

서버에는 최신 `run_kafka_experiments.sh`를 반영하세요. 각 CSV와 같은 태그의 `.run.txt`에 실행 인수·적용 설정·실제 하위 명령·결과 경로·종료 상태를 기록하고, 실행별 `_runs.tsv`에 연결표를 저장합니다.

기존 CSV를 소급해서 기록하는 기능은 아니며 업데이트 이후 실행부터 적용됩니다.
