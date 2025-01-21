#!/bin/bash

# Default configuration file path
CONFIG_FILE="config.yaml"

# Initial variables
JAR_FILE=""
OUTPUT_DIR=""
OUTPUT_SUFFIX=""

# Add options for configuration file path and to control page cache clearing
while getopts c:j:d:f: flag
do
    case "${flag}" in
        c) CONFIG_FILE=${OPTARG};;    # Configuration file path
        j) JAR_FILE=${OPTARG};;       # JAR file path
        d) OUTPUT_DIR=${OPTARG};;     # Output directory
        f) OUTPUT_SUFFIX=${OPTARG};;  # Output file suffix
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
OUTPUT_DIR=${OUTPUT_DIR:-$(read_yaml_value "output_dir")}
OUTPUT_SUFFIX=${OUTPUT_SUFFIX:-$(read_yaml_value "output_suffix")}

# Validation
if [ -z "$JAR_FILE" ] || [ -z "$OUTPUT_DIR" ] || [ -z "$OUTPUT_SUFFIX" ]; then
    echo "Error: Missing required configuration values."
    exit 1
fi

TARGET_FILE="${OUTPUT_DIR}/${OUTPUT_SUFFIX}.csv"
# Check if OUTPUT_DIR exists, if not, create it
if [ ! -e "$TARGET_FILE" ]; then
    echo "There is no target file ($TARGET_FILE)."
    exit 1
fi

echo "Export log to metric file..."
java -cp "$JAR_FILE" org.example.NaiveProducerMetricExporter "${OUTPUT_DIR}/${OUTPUT_SUFFIX}.csv" -o "${OUTPUT_DIR}/${OUTPUT_SUFFIX}_exported.csv"
echo "Export job done."
