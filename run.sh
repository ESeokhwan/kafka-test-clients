#!/usr/bin/env bash
# Run with Bash from PATH; example interpreter: /usr/bin/bash.
set -euo pipefail
# Exit on command failure, unset variables, and pipeline failures; example: stop if javac fails.

KAFKA_HOME="${KAFKA_HOME:-/data4/kafka-tt}"                         # Kafka source/build root; example: /data4/kafka-tt.
LIB_DIR="${KAFKA_HOME}/libs"                                        # Kafka distribution library directory; example: /data4/kafka-tt/libs.
KAFKA_CLIENTS_LIB_DIR="${KAFKA_HOME}/clients/build/libs"            # kafka-clients jar directory; example: /data4/kafka-tt/clients/build/libs.
KAFKA_DEP_LIB_DIR="${KAFKA_HOME}/core/build/dependant-libs-2.13.16" # Kafka dependent jars directory; example: /data4/kafka-tt/core/build/dependant-libs-2.13.16.
CLASSPATH=".:${LIB_DIR}/*:${KAFKA_CLIENTS_LIB_DIR}/*:${KAFKA_DEP_LIB_DIR}/*" # Java classpath; example includes current dir, Kafka libs, kafka-clients, and dependencies.

if ! compgen -G "${KAFKA_CLIENTS_LIB_DIR}/kafka-clients*.jar" > /dev/null; then # Verify kafka-clients jar exists; example match: kafka-clients-*.jar.
    echo "[Error] kafka-clients jar not found under ${KAFKA_CLIENTS_LIB_DIR}"    # Print missing jar path; example: /data4/kafka-tt/clients/build/libs.
    echo "[Hint] Build it first: cd ${KAFKA_HOME} && ./gradlew :clients:jar"     # Print build hint; example command: cd /data4/kafka-tt && ./gradlew :clients:jar.
    exit 1                                                                      # Stop before javac/java because classpath is incomplete; example exit code: 1.
fi                                                                              # End kafka-clients jar existence check.

JMX_HOST="${JMX_HOST:-127.0.0.1}"                                      # Kafka broker JMX host; example override: JMX_HOST=10.0.0.5.
JMX_PORT="${JMX_PORT:-9999}"                                           # Kafka broker JMX port; example override: JMX_PORT=9999.
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-127.0.0.1:9092}"               # Kafka bootstrap servers; example override: BOOTSTRAP_SERVERS=10.0.0.5:9092.
SCAN_INTERVAL_MS="${SCAN_INTERVAL_MS:-1000}"                           # JMX scan interval; example: 1000 scans once per second.
SCAN_LOG_INTERVAL_MS="${SCAN_LOG_INTERVAL_MS:-1000}"                  # Scan summary log interval; example: 60000 prints scan summary once per minute.
INACTIVITY_CHECK_INTERVAL_MS="${INACTIVITY_CHECK_INTERVAL_MS:-10000}" # Inactive-topic evaluation interval; example: 300000 checks once every 5 minutes.
INACTIVITY_THRESHOLD_MS="${INACTIVITY_THRESHOLD_MS:-600000}"           # No-offset-change threshold before deletion; example: 300000 means inactive for 5 minutes.
#INACTIVITY_THRESHOLD_MS="${INACTIVITY_THRESHOLD_MS:-1000}"           # No-offset-change threshold before deletion; example: 300000 means inactive for 5 minutes.
# Valid DELETION_MODE values from KafkaJmxProducerBatchDaemon.DeletionMode:
# - BATCH: collect all inactive topics found in one inactivity check and send one AdminClient.deleteTopics(list) request.
# - IMMEDIATE: enqueue each inactive topic separately and send AdminClient.deleteTopics(single-topic list) one topic at a time.
DELETION_MODE="${DELETION_MODE:-BATCH}"                                # Topic deletion mode passed to Java; examples: BATCH, IMMEDIATE.
#DELETION_MODE="${DELETION_MODE:-IMMEDIATE}"                                # Topic deletion mode passed to Java; examples: BATCH, IMMEDIATE.
CSV_FILE="${CSV_FILE:-topic_metrics.csv}"                              # CSV metrics output file; example: topic_metrics.csv.

JAVA_ARGS=(                              # Positional arguments for KafkaJmxProducerBatchDaemon; order must match Java main().
    "${JMX_HOST}"                        # Arg 1: JMX host; example: 127.0.0.1.
    "${JMX_PORT}"                        # Arg 2: JMX port; example: 9999.
    "${BOOTSTRAP_SERVERS}"               # Arg 3: Kafka bootstrap servers; example: 127.0.0.1:9092.
    "${SCAN_INTERVAL_MS}"                # Arg 4: scan interval in ms; example: 1000.
    "${INACTIVITY_CHECK_INTERVAL_MS}"    # Arg 5: inactivity check interval in ms; example: 300000.
    "${DELETION_MODE}"                   # Arg 6: deletion mode; example: BATCH.
    "${CSV_FILE}"                        # Arg 7: CSV output path; example: topic_metrics.csv.
    "${INACTIVITY_THRESHOLD_MS}"         # Arg 8: inactivity threshold in ms; example: 300000.
    "${SCAN_LOG_INTERVAL_MS}"            # Arg 9: scan log interval in ms; example: 60000.
)                                        # End Java positional argument list.

echo "[Script] Compiling Java source..."                                # Show compile step; example source: KafkaJmxProducerBatchDaemon.java.
javac -encoding UTF-8 -cp "${CLASSPATH}" KafkaJmxProducerBatchDaemon.java # Compile daemon source with Kafka classpath; example output: KafkaJmxProducerBatchDaemon.class.

echo "[Script] Starting daemon..."                           # Show daemon startup step; example main class: KafkaJmxProducerBatchDaemon.
java -cp "${CLASSPATH}" KafkaJmxProducerBatchDaemon "${JAVA_ARGS[@]}" # Start daemon with the positional arguments defined above.

# Example behavior with default values:
# 1. KAFKA_HOME defaults to /data4/kafka-tt, so Kafka jars are loaded from /data4/kafka-tt/libs,
#    /data4/kafka-tt/clients/build/libs, and /data4/kafka-tt/core/build/dependant-libs-2.13.16.
# 2. KafkaJmxProducerBatchDaemon.java is compiled with UTF-8 and the Kafka classpath.
# 3. The daemon connects to JMX at 127.0.0.1:9999 and Kafka bootstrap server 127.0.0.1:9092.
# 4. Every 1000 ms, it scans LogEndOffset MBeans for non-internal topics.
# 5. Every 60000 ms, it prints one scan summary log line.
# 6. Every 300000 ms, it checks whether registered topics have had no offset change for at least 300000 ms.
# 7. With DELETION_MODE=BATCH, if 50 topics are inactive at that check, one delete request containing 50 topics is queued.
# 8. With DELETION_MODE=IMMEDIATE, if 50 topics are inactive at that check, 50 single-topic delete requests are queued.
# 9. Delete latency metrics are written to topic_metrics.csv after delete completion.
#
# Example commands:
# DELETION_MODE=BATCH ./run.sh
# DELETION_MODE=IMMEDIATE ./run.sh
# SCAN_INTERVAL_MS=60000 SCAN_LOG_INTERVAL_MS=60000 DELETION_MODE=BATCH ./run.sh
