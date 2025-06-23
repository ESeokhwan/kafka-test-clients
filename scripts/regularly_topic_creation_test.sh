#!/bin/bash

# Initial variables
CONFIG_FILE=""
JAR_FILE=""
BROKER=""
TOPIC_PREFIX=""
PARTITION_COUNT=""
REPLICATION_FACTOR=""
START_INDEX=""
COUNT=""
INTERVAL=""
IS_ASYNC=""

# Add options for configuration file path and parameters
while getopts c:j:b:p:P:r:s:n:i:a: flag
do
    case "${flag}" in
        c) CONFIG_FILE=${OPTARG};;        # Configuration file path
        j) JAR_FILE=${OPTARG};;          # JAR file path
        b) BROKER=${OPTARG};;            # Kafka Broker
        p) TOPIC_PREFIX=${OPTARG};;      # Topic prefix
        P) PARTITION_COUNT=${OPTARG};;   # Partition count
        r) REPLICATION_FACTOR=${OPTARG};; # Replication factor
        s) START_INDEX=${OPTARG};;       # Starting index for topic names
        n) COUNT=${OPTARG};;             # Number of topics to create
        i) INTERVAL=${OPTARG};;          # Interval in milliseconds
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
PARTITION_COUNT=${PARTITION_COUNT:-$(read_yaml_value "partition_count")}
REPLICATION_FACTOR=${REPLICATION_FACTOR:-$(read_yaml_value "replication_factor")}
START_INDEX=${START_INDEX:-$(read_yaml_value "start_index")}
COUNT=${COUNT:-$(read_yaml_value "count")}
INTERVAL=${INTERVAL:-$(read_yaml_value "interval")}
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
JAVA_CMD="java -cp \"$JAR_FILE\" org.example.RegularlyTopicCreationTest"
JAVA_CMD="$JAVA_CMD $BROKER $TOPIC_PREFIX"

# Add optional parameters if they exist
if [ -n "$PARTITION_COUNT" ]; then
    JAVA_CMD="$JAVA_CMD --partition-count $PARTITION_COUNT"
fi

if [ -n "$REPLICATION_FACTOR" ]; then
    JAVA_CMD="$JAVA_CMD --replication-factor $REPLICATION_FACTOR"
fi

if [ -n "$START_INDEX" ]; then
    JAVA_CMD="$JAVA_CMD --start-index $START_INDEX"
fi

if [ -n "$COUNT" ]; then
    JAVA_CMD="$JAVA_CMD --count $COUNT"
fi

if [ -n "$INTERVAL" ]; then
    JAVA_CMD="$JAVA_CMD --interval $INTERVAL"
fi

if [ -n "$IS_ASYNC" ] && [ "$IS_ASYNC" = "true" ]; then
    JAVA_CMD="$JAVA_CMD --is-async"
fi

# Pre-execution logging
echo "=== Kafka Topic Creator Execution Log ==="
echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
echo "Configuration File: $CONFIG_FILE"
echo "JAR File: $JAR_FILE"
echo "Kafka Broker: $BROKER"
echo "Topic Prefix: $TOPIC_PREFIX"
echo "Partition Count: ${PARTITION_COUNT:-default}"
echo "Replication Factor: ${REPLICATION_FACTOR:-default}"
echo "Start Index: ${START_INDEX:-default}"
echo "Topic Count: ${COUNT:-default}"
echo "Interval (ms): ${INTERVAL:-default}"
echo "Is Async: ${IS_ASYNC:-default}"
echo "Full Command: $JAVA_CMD"
echo "==========================================="

# Java command execution
echo "Starting Kafka Topic Creation Test..."
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
