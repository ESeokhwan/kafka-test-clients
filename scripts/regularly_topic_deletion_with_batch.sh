#!/bin/bash

# Initial variables
CONFIG_FILE=""
JAR_FILE=""
BROKER=""
TOPIC_PREFIX=""
START_INDEX=""
ROUND_COUNT=""
INTERVAL=""
PER_ROUND_COUNT=""
USE_BATCH=""
IS_ASYNC=""

# Add options for configuration file path and parameters
while getopts c:j:b:p:s:n:i:N:B:a: flag
do
    case "${flag}" in
        c) CONFIG_FILE=${OPTARG};;       # Configuration file path
        j) JAR_FILE=${OPTARG};;          # JAR file path
        b) BROKER=${OPTARG};;            # Kafka Broker
        p) TOPIC_PREFIX=${OPTARG};;      # Topic prefix
        s) START_INDEX=${OPTARG};;       # Starting index for topic names
        n) ROUND_COUNT=${OPTARG};;       # Number of rounds
        i) INTERVAL=${OPTARG};;          # Interval in milliseconds
        N) PER_ROUND_COUNT=${OPTARG};;   # Number of topics to delete for each round
        B) USE_BATCH=${OPTARG};;         # Use batch flag (true/false)
        a) IS_ASYNC=${OPTARG};;          # Is async flag (true/false)
    esac
done

# Check if configuration file exists
if [ ! "$CONFIG_FILE" = "" ] && [ ! -f "$CONFIG_FILE" ]; then
    echo "Config file $CONFIG_FILE not found!"
    exit 1
fi

# Function to read values from YAML file
read_yaml_value() {
    local key=$1
    if [ -n "$CONFIG_FILE" ] && [ -f "$CONFIG_FILE" ]; then
        grep -E "^$key:" "$CONFIG_FILE" | sed -E "s/^$key:\s*//" | sed 's/^"\|"$//g'
    else
        echo ""
    fi
}

# Read values from YAML (use command line values if provided, else fallback to config file)
JAR_FILE=${JAR_FILE:-$(read_yaml_value "jar_file")}
BROKER=${BROKER:-$(read_yaml_value "broker")}
TOPIC_PREFIX=${TOPIC_PREFIX:-$(read_yaml_value "topic_prefix")}
START_INDEX=${START_INDEX:-$(read_yaml_value "start_index")}
ROUND_COUNT=${ROUND_COUNT:-$(read_yaml_value "round_count")}
INTERVAL=${INTERVAL:-$(read_yaml_value "interval")}
PER_ROUND_COUNT=${PER_ROUND_COUNT:-$(read_yaml_value "per_round_count")}
USE_BATCH=${USE_BATCH:-$(read_yaml_value "use_batch")}
IS_ASYNC=${IS_ASYNC:-$(read_yaml_value "is_async")}

# Validation for required parameters
if [ -z "$JAR_FILE" ] || [ -z "$BROKER" ] || [ -z "$TOPIC_PREFIX" ]; then
    echo "Error: Missing required configuration values."
    echo "Required: jar_file, broker, topic_prefix"
    exit 1
fi

# Check if JAR file exists
if [ ! -f "$JAR_FILE" ]; then
    echo "Error: JAR file not found: $JAR_FILE"
    exit 1
fi

# Build Java command with arguments
JAVA_CMD="java -cp \"$JAR_FILE\" org.example.RegularlyTopicDeletionWithBatch"
JAVA_CMD="$JAVA_CMD $BROKER $TOPIC_PREFIX"

# Add optional parameters if they exist
if [ -n "$START_INDEX" ]; then
    JAVA_CMD="$JAVA_CMD --start-index $START_INDEX"
fi

if [ -n "$ROUND_COUNT" ]; then
    JAVA_CMD="$JAVA_CMD --round-count $ROUND_COUNT"
fi

if [ -n "$INTERVAL" ]; then
    JAVA_CMD="$JAVA_CMD --interval $INTERVAL"
fi

if [ -n "$PER_ROUND_COUNT" ]; then
    JAVA_CMD="$JAVA_CMD --per-round-count $PER_ROUND_COUNT"
fi

if [ -n "$USE_BATCH" ] && [ "$USE_BATCH" = "true" ]; then
    JAVA_CMD="$JAVA_CMD --use-batch"
fi

if [ -n "$IS_ASYNC" ] && [ "$IS_ASYNC" = "true" ]; then
    JAVA_CMD="$JAVA_CMD --is-async"
fi

# Pre-execution logging
echo "=== Kafka Topic Deleter Execution Log ==="
echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
echo "Configuration File: $CONFIG_FILE"
echo "JAR File: $JAR_FILE"
echo "Kafka Broker: $BROKER"
echo "Topic Prefix: $TOPIC_PREFIX"
echo "Start Index: ${START_INDEX:-default}"
echo "Round Count: ${ROUND_COUNT:-default}"
echo "Interval (ms): ${INTERVAL:-default}"
echo "Per Round Count: ${PER_ROUND_COUNT:-default}"
echo "Use Batch: ${USE_BATCH:-default}"
echo "Is Async: ${IS_ASYNC:-default}"
echo "Full Command: $JAVA_CMD"
echo "==========================================="

# Java command execution
echo "Starting Kafka Topic Deletion Test..."
eval $JAVA_CMD
EXIT_CODE=$?

# Post-execution logging
echo "==========================================="
echo "Execution completed at: $(date '+%Y-%m-%d %H:%M:%S')"
echo "Exit Code: $EXIT_CODE"
if [ $EXIT_CODE -eq 0 ]; then
    echo "Status: SUCCESS"
else
    echo "Status: FAILED"
fi
echo "==========================================="

exit $EXIT_CODE
