#!/bin/bash

# Default configuration file path
CONFIG_FILE="config.yaml"
PID_DIR="/tmp/vmtouch_pids"

# Initial variables
JAR_FILE=""
LOG_FILE=""
TOPIC=""
BROKER=""
MESSAGE_SIZE=""
MESSAGE_COUNT=""
OUTPUT_DIR=""
OUTPUT_SUFFIX=""
CLEAR_CACHE="true"  # Default is to enable page cache clearing

# Add options for configuration file path and to control page cache clearing
while getopts c:j:l:t:b:s:n:d:f:x: flag
do
    case "${flag}" in
        c) CONFIG_FILE=${OPTARG};;    # Configuration file path
        j) JAR_FILE=${OPTARG};;       # JAR file path
        l) LOG_FILE=${OPTARG};;       # Log file path
        t) TOPIC=${OPTARG};;          # Kafka Topic
        b) BROKER=${OPTARG};;         # Kafka Broker
        s) MESSAGE_SIZE=${OPTARG};;   # Message size
        n) MESSAGE_COUNT=${OPTARG};;  # Number of messages
        d) OUTPUT_DIR=${OPTARG};;     # Output directory
        f) OUTPUT_SUFFIX=${OPTARG};;  # Output file suffix
        x) CLEAR_CACHE="false";;      # Disable page cache clearing
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
LOG_FILE=${LOG_FILE:-$(read_yaml_value "log_file")}
TOPIC=${TOPIC:-$(read_yaml_value "topic")}
BROKER=${BROKER:-$(read_yaml_value "broker")}
MESSAGE_SIZE=${MESSAGE_SIZE:-$(read_yaml_value "message_size")}
MESSAGE_COUNT=${MESSAGE_COUNT:-$(read_yaml_value "message_count")}
OUTPUT_DIR=${OUTPUT_DIR:-$(read_yaml_value "output_dir")}
OUTPUT_SUFFIX=${OUTPUT_SUFFIX:-$(read_yaml_value "output_suffix")}
if [ -z "$CLEAR_CACHE" ] || [ "$CLEAR_CACHE" == "true" ]; then
    CLEAR_CACHE=$(read_yaml_value "clear_cache")
fi

# Validation
if [ -z "$JAR_FILE" ] || [ -z "$LOG_FILE" ] || [ -z "$TOPIC" ] || [ -z "$BROKER" ] || [ -z "$MESSAGE_SIZE" ] || [ -z "$MESSAGE_COUNT" ] || [ -z "$OUTPUT_DIR" ] || [ -z "$OUTPUT_SUFFIX" ]; then
    echo "Error: Missing required configuration values."
    exit 1
fi

# Check if OUTPUT_DIR exists, if not, create it
if [ ! -d "$OUTPUT_DIR" ]; then
    echo "Output directory does not exist. Creating $OUTPUT_DIR..."
    mkdir -p "$OUTPUT_DIR"
fi

mkdir -p "$PID_DIR"
TIMESTAMP=$(date +%s)
PID_FILE="$PID_DIR/vmtouch_$TIMESTAMP.pid"

# Page cache clearing function
setup_page_cache() {
    if [[ "$CLEAR_CACHE" == "true" ]]; then
        echo "Removing old records from page cache using vmtouch..."
        vmtouch -e "$LOG_FILE"
    else
        vmtouch -tld "$LOG_FILE" & echo $! > "$PID_FILE"
        echo "Loading old records to page cache using vmtouch (PID: $(cat $PID_FILE))..."
    fi
}

cleanup_vmtouch() {
  if [ -e "$PID_FILE" ]; then
    kill -9 $(cat "$PID_FILE") && rm -f "$PID_FILE"
    echo "Stopped lock on file using vmtouch."
  else
    echo "No running vmtouch process found."
  fi
}

# Clear page cache execution
setup_page_cache

# Java command execution
echo "Running Java Kafka consumer..."
java -cp "$JAR_FILE" org.example.EarliestConsumerWithMonitor "$BROKER" "$TOPIC" -s "$MESSAGE_SIZE" -n "$MESSAGE_COUNT" -m "${OUTPUT_DIR}/${OUTPUT_SUFFIX}.csv"

# Cleanup vmtouch process
cleanup_vmtouch

echo "Execution completed."

