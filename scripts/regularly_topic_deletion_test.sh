#!/bin/bash

# Default configuration file path
CONFIG_FILE="config.yaml"

# Initial variables
JAR_FILE=""
BROKER=""
TOPIC_PREFIX=""
START_INDEX=""
COUNT=""
INTERVAL=""

# Add options for configuration file path and to control page cache clearing
while getopts c:j:b:p:s:c:i: flag
do
    case "${flag}" in
        c) CONFIG_FILE=${OPTARG};;    # Configuration file path
        j) JAR_FILE=${OPTARG};;       # JAR file path
        b) BROKER=${OPTARG};;         # Kafka Broker
        p) TOPIC_PREFIX=${OPTARG};;   # Topic prefix
        s) START_INDEX=${OPTARG};;    # Starting index for topic names
        c) COUNT=${OPTARG};;          # Number of topics to create
        i) INTERVAL=${OPTARG};;       # interval
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
BROKER=${BROKER:-$(read_yaml_value "broker")}
TOPIC_PREFIX=${TOPIC_PREFIX:-$(read_yaml_value "topic_prefix")}
START_INDEX=${START_INDEX:-$(read_yaml_value "start_index")}
COUNT=${COUNT:-$(read_yaml_value "count")}
INTERVAL=${INTERVAL:-$(read_yaml_value "interval")}

# Validation
if [ -z "$JAR_FILE" ] || [ -z "$BROKER" ] || [ -z "$TOPIC_PREFIX" ] || [ -z "$START_INDEX" ] || [ -z "$COUNT" ] || [ -z "$INTERVAL" ]; then
    echo "Error: Missing required configuration values."
    exit 1
fi

# Java command execution
echo "Running Java Kafka consumer..."
java -cp "$JAR_FILE" org.example.RegularlyTopicDeletionTest "$BROKER" "${TOPIC_PREFIX}" -s "$START_INDEX" -c "$COUNT" -i "$INTERVAL"

echo "Execution completed."

