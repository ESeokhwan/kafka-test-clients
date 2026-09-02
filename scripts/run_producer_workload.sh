#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

if [[ -z "${CLIENT_DIR:-}" ]]; then
    if [[ -f "${SCRIPT_DIR}/kafka_clients_run.sh" && -d "${SCRIPT_DIR}/../build/libs" ]]; then
        CLIENT_DIR="$(cd "${SCRIPT_DIR}/.." &> /dev/null && pwd)"
    elif [[ -f "${SCRIPT_DIR}/scripts/kafka_clients_run.sh" && -d "${SCRIPT_DIR}/build/libs" ]]; then
        CLIENT_DIR="${SCRIPT_DIR}"
    elif [[ -f "${SCRIPT_DIR}/kafka-test-clients_extracted/kafka-test-clients/scripts/kafka_clients_run.sh" ]]; then
        CLIENT_DIR="${SCRIPT_DIR}/kafka-test-clients_extracted/kafka-test-clients"
    else
        CLIENT_DIR="${SCRIPT_DIR}/kafka-test-clients_extracted/kafka-test-clients"
    fi
fi

RUNNER="${RUNNER:-${CLIENT_DIR}/scripts/kafka_clients_run.sh}"
COMMAND="${COMMAND:-CreateTopicAndProduceWorkload}"

BROKERS="${BROKERS:-166.104.110.11:9092}"
PREFIX="${PREFIX:-test_creator}"
START_INDEX="${START_INDEX:-0}"

if [[ $# -gt 0 && "$1" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    TOPIC_CREATE_HZ="$1"
    shift
else
    TOPIC_CREATE_HZ="${TOPIC_CREATE_HZ:-5}"
fi

TOPIC_CNT="${TOPIC_CNT:--1}"
RUN_DURATION_MINUTES="${RUN_DURATION_MINUTES:-20}"
TOPIC_LIFETIME_MS="${TOPIC_LIFETIME_MS:-60000}"
PRODUCE_HZ_PER_TOPIC="${PRODUCE_HZ_PER_TOPIC:-100}"
MSG_SIZE="${MSG_SIZE:-1000}"
START_BARRIER_DELAY_MS="${START_BARRIER_DELAY_MS:-10000}"
if [[ "${SCRIPT_DIR}" == "${CLIENT_DIR}" || "${SCRIPT_DIR}" == "${CLIENT_DIR}/scripts" ]]; then
    DEFAULT_OUTPUT_DIR="${CLIENT_DIR}/renewed_output/auto_topic_producer_metrics"
else
    DEFAULT_OUTPUT_DIR="${SCRIPT_DIR}/renewed_output/auto_topic_producer_metrics"
fi
OUTPUT_DIR="${OUTPUT_DIR:-${DEFAULT_OUTPUT_DIR}}"

STATS_DISABLED="${STATS_DISABLED:-false}"

IGNORE_RESPONSE=false
for arg in "$@"; do
    if [[ "${arg}" == "--ignore-response" ]]; then
        IGNORE_RESPONSE=true
    fi
done

if [[ ! -f "${RUNNER}" ]]; then
    echo "ERROR: kafka client runner not found: ${RUNNER}" >&2
    echo "Set CLIENT_DIR=/path/to/kafka-test-clients or RUNNER=/path/to/kafka_clients_run.sh." >&2
    exit 1
fi

awk -v hz="${TOPIC_CREATE_HZ}" 'BEGIN { exit !(hz > 0 && hz <= 20) }' || {
    echo "ERROR: topic create rate must be > 0 and <= 20 Hz. value=${TOPIC_CREATE_HZ}" >&2
    exit 1
}

DERIVED_TOPIC_CNT="$(awk -v configured="${TOPIC_CNT}" -v hz="${TOPIC_CREATE_HZ}" -v minutes="${RUN_DURATION_MINUTES}" '
    function ceil(value) { return int(value) + (value > int(value)) }
    BEGIN {
        if (configured > 0) {
            print configured
        } else {
            print ceil(hz * 60 * minutes)
        }
    }
')"
TOPIC_CREATE_INTERVAL_MS="$(awk -v hz="${TOPIC_CREATE_HZ}" 'BEGIN { printf "%.3f", 1000 / hz }')"
EXPECTED_ACTIVE_TOPICS="$(awk -v hz="${TOPIC_CREATE_HZ}" -v lifetimeMs="${TOPIC_LIFETIME_MS}" '
    function ceil(value) { return int(value) + (value > int(value)) }
    BEGIN { print ceil(hz * lifetimeMs / 1000) }
')"
EXPECTED_MESSAGES_PER_TOPIC="$(awk -v lifetimeMs="${TOPIC_LIFETIME_MS}" -v produceHz="${PRODUCE_HZ_PER_TOPIC}" '
    BEGIN { printf "%.0f", lifetimeMs * produceHz / 1000 }
')"
EXPECTED_PRODUCE_MESSAGES="$(awk -v topics="${DERIVED_TOPIC_CNT}" -v perTopic="${EXPECTED_MESSAGES_PER_TOPIC}" '
    BEGIN { printf "%.0f", topics * perTopic }
')"

mkdir -p "${OUTPUT_DIR}"
TIMESTAMP="$(date +"%Y%m%d_%H%M%S")"
METRICS_FILE="${METRICS_FILE:-${OUTPUT_DIR}/${TOPIC_CREATE_HZ}Hz_metrics_${TIMESTAMP}.csv}"

echo "================================================="
echo "Auto-topic producer metrics"
echo "Client dir            : ${CLIENT_DIR}"
echo "Runner                : ${RUNNER}"
echo "Command               : ${COMMAND}"
echo "Brokers               : ${BROKERS}"
echo "Prefix                : ${PREFIX}"
echo "Topic create rate     : ${TOPIC_CREATE_HZ} Hz"
echo "Topic create interval : ${TOPIC_CREATE_INTERVAL_MS} ms"
echo "Run duration          : ${RUN_DURATION_MINUTES} min"
echo "Topic lifetime        : ${TOPIC_LIFETIME_MS} ms"
echo "Produce rate/topic    : ${PRODUCE_HZ_PER_TOPIC} Hz"
echo "Ignore response       : ${IGNORE_RESPONSE}"
echo "Derived topic count   : ${DERIVED_TOPIC_CNT}"
echo "Expected active topics: ${EXPECTED_ACTIVE_TOPICS}"
echo "Expected messages/topic: ${EXPECTED_MESSAGES_PER_TOPIC}"
echo "Expected produce messages: ${EXPECTED_PRODUCE_MESSAGES}"
echo "Metrics CSV           : ${METRICS_FILE}"
echo "================================================="

ARGS=(
    sandbox ${COMMAND}
    -b "${BROKERS}"
    -p "${PREFIX}"
    --start-index "${START_INDEX}"
    --topic-cnt "${TOPIC_CNT}"
    --topic-create-hz "${TOPIC_CREATE_HZ}"
    --run-duration-minutes "${RUN_DURATION_MINUTES}"
    --topic-lifetime-ms "${TOPIC_LIFETIME_MS}"
    --produce-hz-per-topic "${PRODUCE_HZ_PER_TOPIC}"
    --msg-size "${MSG_SIZE}"
    --start-barrier-delay "${START_BARRIER_DELAY_MS}"
    --metrics-file "${METRICS_FILE}"
)

if [[ "${STATS_DISABLED}" == "true" ]]; then
    ARGS+=(--stats-disabled)
fi

bash "${RUNNER}" "${ARGS[@]}" "$@"
