#!/usr/bin/env bash
# 오류 발생, 미정의 변수 사용, 파이프라인 실패 시 즉시 종료한다.
set -euo pipefail

# 이 스크립트 파일이 있는 절대 경로를 구한다.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

# CLIENT_DIR이 외부에서 지정되지 않았을 때만 자동으로 탐색한다.
if [[ -z "${CLIENT_DIR:-}" ]]; then
    # 스크립트가 clients 디렉터리의 scripts 하위에 있는 경우를 처리한다.
    if [[ -f "${SCRIPT_DIR}/kafka_clients_run.sh" && -d "${SCRIPT_DIR}/../build/libs" ]]; then
        # 상위 디렉터리를 Kafka client 프로젝트 경로로 사용한다.
        CLIENT_DIR="$(cd "${SCRIPT_DIR}/.." &> /dev/null && pwd)"
    # 스크립트가 Kafka client 프로젝트 최상위에 있는 경우를 처리한다.
    elif [[ -f "${SCRIPT_DIR}/scripts/kafka_clients_run.sh" && -d "${SCRIPT_DIR}/build/libs" ]]; then
        # 현재 스크립트 디렉터리를 Kafka client 프로젝트 경로로 사용한다.
        CLIENT_DIR="${SCRIPT_DIR}"
    # 압축 해제된 Kafka client 경로가 존재하는 경우를 처리한다.
    elif [[ -f "${SCRIPT_DIR}/kafka-test-clients_extracted/kafka-test-clients/scripts/kafka_clients_run.sh" ]]; then
        # 압축 해제된 Kafka client 프로젝트 경로를 사용한다.
        CLIENT_DIR="${SCRIPT_DIR}/kafka-test-clients_extracted/kafka-test-clients"
    # 위 조건을 찾지 못해도 기존 기본 경로를 사용하도록 한다.
    else
        # 사용자가 CLIENT_DIR 또는 RUNNER를 명시할 수 있도록 예상 기본 경로를 설정한다.
        CLIENT_DIR="${SCRIPT_DIR}/kafka-test-clients_extracted/kafka-test-clients"
    # CLIENT_DIR 자동 탐색 조건을 끝낸다.
    fi
# CLIENT_DIR 자동 탐색 여부 조건을 끝낸다.
fi

# 외부 RUNNER가 있으면 사용하고, 없으면 Kafka client의 기본 실행기를 사용한다.
RUNNER="${RUNNER:-${CLIENT_DIR}/scripts/kafka_clients_run.sh}"

# Kafka 브로커 주소의 기본값을 설정한다.
BROKERS="${BROKERS:-166.104.110.11:9092}"
# 생성할 토픽 이름의 접두어 기본값을 설정한다.
PREFIX="${PREFIX:-test_creator}"
# 토픽 번호 시작 인덱스의 기본값을 설정한다.
START_INDEX="${START_INDEX:-0}"

# 첫 번째 인자가 숫자이면 해당 Hz 하나만 실행하도록 사용한다.
if [[ $# -gt 0 && "$1" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    # 숫자 인자를 주파수 목록으로 저장한다.
    TOPIC_CREATE_HZ_LIST="$1"
    # 처리한 숫자 인자를 이후 실행기로 전달하지 않도록 제거한다.
    shift
# TOPIC_CREATE_HZ_LIST가 명시되었으면 여러 주파수 목록을 그대로 사용한다.
elif [[ -n "${TOPIC_CREATE_HZ_LIST:-}" ]]; then
    # 별도의 동작 없이, 명시된 목록의 우선순위를 유지한다.
    :
# 이전 단일 주파수 환경 변수 인터페이스도 계속 지원한다.
elif [[ -n "${TOPIC_CREATE_HZ:-}" ]]; then
    # 단일 주파수를 목록 형식으로 변환한다.
    TOPIC_CREATE_HZ_LIST="${TOPIC_CREATE_HZ}"
# 어떤 주파수도 지정되지 않으면 기본 1~5 Hz 실험을 설정한다.
else
    # 1 Hz부터 5 Hz까지 순서대로 실행할 기본 목록을 설정한다.
    TOPIC_CREATE_HZ_LIST="1 2 3 4 5"
# 주파수 목록 선택 조건을 끝낸다.
fi

# 목록에서 쉼표를 공백으로 바꿔 "1,2,3" 형식도 허용한다.
TOPIC_CREATE_HZ_LIST="${TOPIC_CREATE_HZ_LIST//,/ }"
# 공백으로 구분된 주파수 문자열을 Bash 배열로 변환한다.
read -r -a TOPIC_CREATE_HZES <<< "${TOPIC_CREATE_HZ_LIST}"
# 변환 결과가 비어 있으면 실행할 실험이 없으므로 오류를 낸다.
if [[ ${#TOPIC_CREATE_HZES[@]} -eq 0 ]]; then
    # 오류 원인을 표준 오류로 출력한다.
    echo "ERROR: TOPIC_CREATE_HZ_LIST must contain at least one rate." >&2
    # 오류 상태로 스크립트를 끝낸다.
    exit 1
# 빈 주파수 목록 검사 조건을 끝낸다.
fi

# 숫자 주파수 인자를 제외한 사용자 옵션을 별도 배열로 보존한다.
EXTRA_ARGS=("$@")

# 생성할 토픽 수의 기본값(-1: 주파수와 실행 시간으로 자동 계산)을 설정한다.
TOPIC_CNT="${TOPIC_CNT:--1}"
# 각 주파수 실험의 실행 시간(분) 기본값을 설정한다.
RUN_DURATION_MINUTES="${RUN_DURATION_MINUTES:-60}"
# 토픽의 생존 시간(밀리초) 기본값을 설정한다.
TOPIC_LIFETIME_MS="${TOPIC_LIFETIME_MS:-40000}"
# 토픽 하나당 메시지 전송 주파수 기본값을 설정한다.
PRODUCE_HZ_PER_TOPIC="${PRODUCE_HZ_PER_TOPIC:-100}"
# 메시지 크기(바이트) 기본값을 설정한다.
MSG_SIZE="${MSG_SIZE:-4096}"
# 각 실험 내부의 시작 배리어 지연 시간(밀리초) 기본값을 설정한다.
START_BARRIER_DELAY_MS="${START_BARRIER_DELAY_MS:-10000}"
# 다음 주파수 실험 전 대기 시간(초) 기본값을 60초로 설정한다.
INTER_EXPERIMENT_DELAY_SECONDS="${INTER_EXPERIMENT_DELAY_SECONDS:-60}"

# 실험 사이 대기 시간이 0 이상의 정수인지 검사한다.
if ! [[ "${INTER_EXPERIMENT_DELAY_SECONDS}" =~ ^[0-9]+$ ]]; then
    # 잘못된 대기 시간 값을 표준 오류로 출력한다.
    echo "ERROR: INTER_EXPERIMENT_DELAY_SECONDS must be a non-negative integer. value=${INTER_EXPERIMENT_DELAY_SECONDS}" >&2
    # 잘못된 설정이므로 오류 상태로 스크립트를 끝낸다.
    exit 1
# 실험 사이 대기 시간 검사 조건을 끝낸다.
fi

# 스크립트 위치와 client 위치 관계에 맞는 기본 결과 디렉터리를 선택한다.
if [[ "${SCRIPT_DIR}" == "${CLIENT_DIR}" || "${SCRIPT_DIR}" == "${CLIENT_DIR}/scripts" ]]; then
    # Kafka client 프로젝트 내부에서 실행하는 경우 client 하위 디렉터리에 결과를 저장한다.
    DEFAULT_OUTPUT_DIR="${CLIENT_DIR}/renewed_output/auto_topic_producer_metrics"
# 스크립트가 별도 위치에 있는 경우를 처리한다.
else
    # 스크립트 디렉터리 하위에 결과를 저장한다.
    DEFAULT_OUTPUT_DIR="${SCRIPT_DIR}/renewed_output/auto_topic_producer_metrics"
# 기본 결과 디렉터리 선택 조건을 끝낸다.
fi
# OUTPUT_DIR이 명시되었으면 사용하고, 없으면 위 기본 디렉터리를 사용한다.
OUTPUT_DIR="${OUTPUT_DIR:-${DEFAULT_OUTPUT_DIR}}"

# Kafka client 통계 출력을 비활성화할지 여부의 기본값을 설정한다.
STATS_DISABLED="${STATS_DISABLED:-false}"

# --ignore-response 전달 여부를 기록할 변수를 기본 false로 설정한다.
IGNORE_RESPONSE=false
# 보존한 사용자 옵션을 순회한다.
for arg in "${EXTRA_ARGS[@]}"; do
    # 현재 인수가 --ignore-response인지 확인한다.
    if [[ "${arg}" == "--ignore-response" ]]; then
        # 해당 옵션이 있으면 출력용 상태를 true로 변경한다.
        IGNORE_RESPONSE=true
    # --ignore-response 비교 조건을 끝낸다.
    fi
# 모든 추가 인수의 순회를 끝낸다.
done

# 실제 Kafka client 실행기 파일이 존재하는지 확인한다.
if [[ ! -f "${RUNNER}" ]]; then
    # 찾지 못한 실행기 경로를 표준 오류로 출력한다.
    echo "ERROR: kafka client runner not found: ${RUNNER}" >&2
    # 환경 변수로 경로를 직접 설정하는 방법을 안내한다.
    echo "Set CLIENT_DIR=/path/to/kafka-test-clients or RUNNER=/path/to/kafka_clients_run.sh." >&2
    # 실행기를 찾지 못했으므로 오류 상태로 스크립트를 끝낸다.
    exit 1
# 실행기 파일 검사 조건을 끝낸다.
fi

# 결과 CSV를 저장할 기본 디렉터리가 없으면 생성한다.
mkdir -p "${OUTPUT_DIR}"
# 사용자가 METRICS_FILE을 지정했는지 보관해 파일명 생성에 사용한다.
METRICS_FILE_TEMPLATE="${METRICS_FILE:-}"
# 이번 전체 스윕을 하나의 묶음으로 식별할 타임스탬프를 생성한다.
SWEEP_TIMESTAMP="$(date +"%Y%m%d_%H%M%S")"

# 특정 Hz 실험에 사용할 고유한 metrics CSV 경로를 출력하는 함수를 정의한다.
metrics_file_for_rate() {
    # 함수 첫 번째 인자인 현재 실험 주파수를 지역 변수에 저장한다.
    local hz="$1"

    # 사용자 지정 파일 경로가 없으면 기본 파일명 규칙을 사용한다.
    if [[ -z "${METRICS_FILE_TEMPLATE}" ]]; then
        # 주파수와 스윕 타임스탬프를 포함한 CSV 경로를 출력한다.
        printf '%s/%sHz_metrics_%s.csv' "${OUTPUT_DIR}" "${hz}" "${SWEEP_TIMESTAMP}"
        # 파일 경로 생성을 마쳤으므로 함수를 끝낸다.
        return
    # 사용자 지정 파일 경로 유무 조건을 끝낸다.
    fi

    # 단일 실험이면 사용자가 지정한 파일 경로를 변경하지 않는다.
    if [[ ${#TOPIC_CREATE_HZES[@]} -eq 1 ]]; then
        # 지정된 CSV 경로를 그대로 출력한다.
        printf '%s' "${METRICS_FILE_TEMPLATE}"
        # 파일 경로 생성을 마쳤으므로 함수를 끝낸다.
        return
    # 단일 실험 여부 조건을 끝낸다.
    fi

    # 사용자 지정 파일명을 주파수 접미사를 붙일 구성 요소로 나눈다.
    local metrics_dir metrics_name metrics_stem metrics_ext
    # 사용자 지정 경로에서 디렉터리 부분을 추출한다.
    metrics_dir="$(dirname "${METRICS_FILE_TEMPLATE}")"
    # 사용자 지정 경로에서 파일명 부분을 추출한다.
    metrics_name="$(basename "${METRICS_FILE_TEMPLATE}")"
    # 일반적인 확장자가 있는 파일명인지 확인한다.
    if [[ "${metrics_name}" == *.* && "${metrics_name}" != .* ]]; then
        # 마지막 확장자를 제외한 파일명 부분을 추출한다.
        metrics_stem="${metrics_name%.*}"
        # 마지막 확장자를 보존한다.
        metrics_ext=".${metrics_name##*.}"
    # 확장자가 없거나 숨김 파일명인 경우를 처리한다.
    else
        # 파일명 전체를 기본 이름으로 사용한다.
        metrics_stem="${metrics_name}"
        # 추가할 확장자는 없도록 설정한다.
        metrics_ext=""
    # 확장자 유무 조건을 끝낸다.
    fi
    # 파일명과 확장자 사이에 Hz를 넣어 각 실험 파일을 구분한다.
    printf '%s/%s_%sHz%s' "${metrics_dir}" "${metrics_stem}" "${hz}" "${metrics_ext}"
# metrics 파일 경로 생성 함수를 끝낸다.
}

# 한 개 주파수에 대한 Kafka AutoTopicProducerMetrics 실험을 실행하는 함수를 정의한다.
run_experiment() {
    # 함수 첫 번째 인자인 현재 실험 주파수를 지역 변수에 저장한다.
    local hz="$1"
    # 토픽 수, 간격, 활성 토픽 수를 저장할 지역 변수를 선언한다.
    local derived_topic_cnt topic_create_interval_ms expected_active_topics
    # 메시지 예상치와 결과 파일 경로를 저장할 지역 변수를 선언한다.
    local expected_messages_per_topic expected_produce_messages metrics_file
    # Kafka client 실행 인수를 저장할 지역 배열을 선언한다.
    local -a args

    # 현재 주파수가 0보다 크고 20 Hz 이하인지 검사한다.
    awk -v hz="${hz}" 'BEGIN { exit !(hz > 0 && hz <= 20) }' || {
        # 허용 범위를 벗어난 주파수를 표준 오류로 출력한다.
        echo "ERROR: topic create rate must be > 0 and <= 20 Hz. value=${hz}" >&2
        # 현재 실험을 실패 처리한다.
        return 1
    # 주파수 유효성 검사 실패 처리 블록을 끝낸다.
    }

    # TOPIC_CNT가 양수가 아니면 현재 주파수와 실행 시간으로 토픽 수를 올림 계산한다.
    derived_topic_cnt="$(awk -v configured="${TOPIC_CNT}" -v hz="${hz}" -v minutes="${RUN_DURATION_MINUTES}" '
        # 소수점 결과를 올림하기 위한 AWK 함수를 정의한다.
        function ceil(value) { return int(value) + (value > int(value)) }
        # AWK 계산을 시작한다.
        BEGIN {
            # 사용자가 양수 토픽 수를 지정했는지 확인한다.
            if (configured > 0) {
                # 지정된 토픽 수를 그대로 출력한다.
                print configured
            # 자동 계산이 필요한 경우를 처리한다.
            } else {
                # Hz × 60초 × 실행 시간(분)을 올림해 토픽 수를 출력한다.
                print ceil(hz * 60 * minutes)
            # 토픽 수 계산 조건을 끝낸다.
            }
        # AWK 시작 블록을 끝낸다.
        }
    ')"
    # 현재 주파수의 토픽 생성 간격을 밀리초 단위로 계산한다.
    topic_create_interval_ms="$(awk -v hz="${hz}" 'BEGIN { printf "%.3f", 1000 / hz }')"
    # 토픽 생존 시간 동안 동시에 활성화될 것으로 예상되는 토픽 수를 올림 계산한다.
    expected_active_topics="$(awk -v hz="${hz}" -v lifetimeMs="${TOPIC_LIFETIME_MS}" '
        # 소수점 결과를 올림하기 위한 AWK 함수를 정의한다.
        function ceil(value) { return int(value) + (value > int(value)) }
        # 주파수 × 생존 시간을 토픽 수로 환산해 출력한다.
        BEGIN { print ceil(hz * lifetimeMs / 1000) }
    ')"
    # 토픽 하나가 생존하는 동안 전송할 예상 메시지 수를 계산한다.
    expected_messages_per_topic="$(awk -v lifetimeMs="${TOPIC_LIFETIME_MS}" -v produceHz="${PRODUCE_HZ_PER_TOPIC}" '
        # 밀리초 단위 생존 시간에 전송 주파수를 적용해 정수 메시지 수를 출력한다.
        BEGIN { printf "%.0f", lifetimeMs * produceHz / 1000 }
    ')"
    # 전체 실험에서 전송할 예상 메시지 수를 계산한다.
    expected_produce_messages="$(awk -v topics="${derived_topic_cnt}" -v perTopic="${expected_messages_per_topic}" '
        # 토픽 수와 토픽당 예상 메시지 수를 곱해 출력한다.
        BEGIN { printf "%.0f", topics * perTopic }
    ')"
    # 현재 주파수용 고유 metrics CSV 경로를 생성한다.
    metrics_file="$(metrics_file_for_rate "${hz}")"

    # 실험 설정 출력의 구분선을 표시한다.
    echo "================================================="
    # 현재 실행할 Auto-topic producer metrics 실험 주파수를 표시한다.
    echo "Auto-topic producer metrics (${hz} Hz)"
    # Kafka client 프로젝트 경로를 표시한다.
    echo "Client dir            : ${CLIENT_DIR}"
    # 실제 실행기 경로를 표시한다.
    echo "Runner                : ${RUNNER}"
    # 실행할 애플리케이션 이름을 표시한다.
    echo "Command               : AutoTopicProducerMetrics"
    # Kafka 브로커 주소를 표시한다.
    echo "Brokers               : ${BROKERS}"
    # 토픽 이름 접두어를 표시한다.
    echo "Prefix                : ${PREFIX}"
    # 현재 토픽 생성 주파수를 표시한다.
    echo "Topic create rate     : ${hz} Hz"
    # 현재 토픽 생성 간격을 표시한다.
    echo "Topic create interval : ${topic_create_interval_ms} ms"
    # 실험 실행 시간을 표시한다.
    echo "Run duration          : ${RUN_DURATION_MINUTES} min"
    # 토픽 생존 시간을 표시한다.
    echo "Topic lifetime        : ${TOPIC_LIFETIME_MS} ms"
    # 토픽당 메시지 전송 주파수를 표시한다.
    echo "Produce rate/topic    : ${PRODUCE_HZ_PER_TOPIC} Hz"
    # 응답 무시 옵션의 사용 여부를 표시한다.
    echo "Ignore response       : ${IGNORE_RESPONSE}"
    # 계산되거나 지정된 토픽 수를 표시한다.
    echo "Derived topic count   : ${derived_topic_cnt}"
    # 예상 동시 활성 토픽 수를 표시한다.
    echo "Expected active topics: ${expected_active_topics}"
    # 토픽당 예상 메시지 수를 표시한다.
    echo "Expected messages/topic: ${expected_messages_per_topic}"
    # 전체 예상 메시지 수를 표시한다.
    echo "Expected produce messages: ${expected_produce_messages}"
    # 현재 실험의 결과 CSV 경로를 표시한다.
    echo "Metrics CSV           : ${metrics_file}"
    # 실험 설정 출력의 구분선을 마무리한다.
    echo "================================================="

    # Kafka client 실행기에 전달할 기본 인수 배열을 만든다.
    args=(
        # 실행할 애플리케이션을 지정한다.
        apps AutoTopicProducerMetrics
        # Kafka 브로커 주소를 전달한다.
        -b "${BROKERS}"
        # 토픽 이름 접두어를 전달한다.
        -p "${PREFIX}"
        # 토픽 번호 시작 인덱스를 전달한다.
        --start-index "${START_INDEX}"
        # 생성할 토픽 수 또는 자동 계산 옵션을 전달한다.
        --topic-cnt "${TOPIC_CNT}"
        # 현재 실험의 토픽 생성 주파수를 전달한다.
        --topic-create-hz "${hz}"
        # 실험 실행 시간을 분 단위로 전달한다.
        --run-duration-minutes "${RUN_DURATION_MINUTES}"
        # 토픽 생존 시간을 밀리초 단위로 전달한다.
        --topic-lifetime-ms "${TOPIC_LIFETIME_MS}"
        # 토픽당 메시지 전송 주파수를 전달한다.
        --produce-hz-per-topic "${PRODUCE_HZ_PER_TOPIC}"
        # 메시지 크기를 바이트 단위로 전달한다.
        --msg-size "${MSG_SIZE}"
        # 실험 시작 배리어 지연 시간을 전달한다.
        --start-barrier-delay "${START_BARRIER_DELAY_MS}"
        # 현재 주파수에 분리된 metrics CSV 경로를 전달한다.
        --metrics-file "${metrics_file}"
    # Kafka client 실행기 인수 배열 선언을 끝낸다.
    )

    # 통계 비활성화가 요청되었는지 확인한다.
    if [[ "${STATS_DISABLED}" == "true" ]]; then
        # 통계 비활성화 플래그를 실행기 인수에 추가한다.
        args+=(--stats-disabled)
    # 통계 비활성화 조건을 끝낸다.
    fi

    # 준비한 인수와 함수 밖에서 보존한 사용자 옵션으로 Kafka client 실행기를 호출한다.
    bash "${RUNNER}" "${args[@]}" "${EXTRA_ARGS[@]}"
# 단일 주파수 실험 실행 함수를 끝낸다.
}

# 실행할 모든 주파수와 실험 간 대기 시간을 먼저 표시한다.
echo "Running topic-create-rate experiments sequentially: ${TOPIC_CREATE_HZES[*]} Hz"
# 각 실험 사이에 적용할 대기 시간을 표시한다.
echo "Inter-experiment delay: ${INTER_EXPERIMENT_DELAY_SECONDS} seconds"

# 주파수 배열의 인덱스를 0부터 마지막까지 순회한다.
for ((experiment_index = 0; experiment_index < ${#TOPIC_CREATE_HZES[@]}; experiment_index++)); do
    # 현재 인덱스에 해당하는 토픽 생성 주파수를 가져온다.
    topic_create_hz="${TOPIC_CREATE_HZES[experiment_index]}"
    # 현재 주파수 실험을 완료될 때까지 실행한다.
    run_experiment "${topic_create_hz}"

    # 마지막 실험이 아니고 대기 시간이 0이 아닐 때만 다음 실험 전 대기한다.
    if [[ ! "${INTER_EXPERIMENT_DELAY_SECONDS}" =~ ^0+$ && ${experiment_index} -lt $((${#TOPIC_CREATE_HZES[@]} - 1)) ]]; then
        # 다음에 실행할 주파수를 출력용 변수에 저장한다.
        next_topic_create_hz="${TOPIC_CREATE_HZES[experiment_index + 1]}"
        # 현재 실험과 다음 실험 사이에 기다릴 시간을 안내한다.
        echo "Waiting ${INTER_EXPERIMENT_DELAY_SECONDS} seconds before the ${next_topic_create_hz} Hz experiment..."
        # 설정된 초만큼 대기해 이전 실험의 토픽 정리 시간을 확보한다.
        sleep "${INTER_EXPERIMENT_DELAY_SECONDS}"
    # 실험 간 대기 조건을 끝낸다.
    fi
# 모든 주파수 실험의 순회를 끝낸다.
done
