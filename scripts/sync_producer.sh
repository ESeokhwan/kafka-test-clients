#!/bin/bash

# Default configuration file path
CONFIG_FILE="config.yaml"

# Initial variables
JAR_FILE=""
TOPIC=""
BROKER=""
MESSAGE_SIZE=""
MESSAGE_COUNT=""
OUTPUT_DIR=""
OUTPUT_SUFFIX=""
WITH_EXPORT="false" # Default is to disable export

# Add options for configuration file path and to control page cache clearing
while getopts c:j:t:b:s:n:d:f:e: flag
do
    case "${flag}" in
        c) CONFIG_FILE=${OPTARG};;    # Configuration file path
        j) JAR_FILE=${OPTARG};;       # JAR file path
        t) TOPIC=${OPTARG};;          # Kafka Topic
        b) BROKER=${OPTARG};;         # Kafka Broker
        s) MESSAGE_SIZE=${OPTARG};;   # Message size
        n) MESSAGE_COUNT=${OPTARG};;  # Number of messages
        d) OUTPUT_DIR=${OPTARG};;     # Output directory
        f) OUTPUT_SUFFIX=${OPTARG};;  # Output file suffix
        e) WITH_EXPORT="true";;
    esac
done

# Check if configuration file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Config file $CONFIG_FILE not found!"
    exit 1
fi

# Function to read values from YAML file
read_yaml_value() {
    local key=$1
    grep -E "^$key:" "$CONFIG_FILE" | sed -E "s/^$key:\s*//"
}

# Read values from YAML (use command line values if provided, else fallback to config file)
JAR_FILE=${JAR_FILE:-$(read_yaml_value "jar_file")}
TOPIC=${TOPIC:-$(read_yaml_value "topic")}
BROKER=${BROKER:-$(read_yaml_value "broker")}
MESSAGE_SIZE=${MESSAGE_SIZE:-$(read_yaml_value "message_size")}
MESSAGE_COUNT=${MESSAGE_COUNT:-$(read_yaml_value "message_count")}
OUTPUT_DIR=${OUTPUT_DIR:-$(read_yaml_value "output_dir")}
OUTPUT_SUFFIX=${OUTPUT_SUFFIX:-$(read_yaml_value "output_suffix")}
if [ "$WITH_EXPORT" == "false" ]; then
    WITH_EXPORT=$(read_yaml_value "with_export")
fi

# Validation
if [ -z "$JAR_FILE" ] || [ -z "$TOPIC" ] || [ -z "$BROKER" ] || [ -z "$MESSAGE_SIZE" ] || [ -z "$MESSAGE_COUNT" ] || [ -z "$OUTPUT_DIR" ] || [ -z "$OUTPUT_SUFFIX" ]; then
    echo "Error: Missing required configuration values."
    exit 1
fi

# Check if OUTPUT_DIR exists, if not, create it
if [ ! -d "$OUTPUT_DIR" ]; then
    echo "Output directory does not exist. Creating $OUTPUT_DIR..."
    mkdir -p "$OUTPUT_DIR"
fi

echo "Running Java Kafka producer..."
java -cp "$JAR_FILE" org.example.SyncProducerWithMonitor "$BROKER" "$TOPIC" -s "$MESSAGE_SIZE" -n "$MESSAGE_COUNT" -m "${OUTPUT_DIR}/${OUTPUT_SUFFIX}.csv"
echo "Producer job done."

if [[ "$WITH_EXPORT" == "true" ]]; then
    echo "Sleep a 5s..."
    sleep 5

    echo "Export log to metric file..."
    java -cp "$JAR_FILE" org.example.BasicMetricExporter "${OUTPUT_DIR}/${OUTPUT_SUFFIX}.csv" -o "${OUTPUT_DIR}/${OUTPUT_SUFFIX}_exported.csv"
    echo "Export job done."
fi

echo "Execution completed."
