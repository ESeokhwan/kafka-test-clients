#!/bin/bash

# Default configuration file path
CONFIG_FILE="config.yaml"

# Initial variables
JAR_FILE=""
P_FILE_PATH=""
C_FILE_PATH=""
OUTPUT_FILE_PATH=""

# Add options for configuration file path and to control page cache clearing
while getopts c:j:p:f:o: flag
do
    case "${flag}" in
        c) CONFIG_FILE=${OPTARG};;    # Configuration file path
        j) JAR_FILE=${OPTARG};;       # JAR file path
        p) P_FILE_PATH=${OPTARG};;     # Producer file path
        f) C_FILE_PATH=${OPTARG};;  # Consumer file path
        o) OUTPUT_FILE_PATH=${OPTARG};; # Output file path
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
P_FILE_PATH=${P_FILE_PATH:-$(read_yaml_value "p_file_path")}
C_FILE_PATH=${C_FILE_PATH:-$(read_yaml_value "c_file_path")}
OUTPUT_FILE_PATH=${OUTPUT_FILE_PATH:-$(read_yaml_value "output_file_path")}

# Validation
if [ -z "$JAR_FILE" ] || [ -z "$P_FILE_PATH" ] || [ -z "$C_FILE_PATH" ] || [ -z "$OUTPUT_FILE_PATH" ]; then
    echo "Error: Missing required configuration values."
    exit 1
fi

# Check if OUTPUT_DIR exists, if not, create it
if [ ! -e "$OUTPUT_FILE_PATH" ]; then
    echo "There is no target file ($OUTPUT_FILE_PATH)."
    exit 1
fi

echo "Export log to metric file..."
java -cp "$JAR_FILE" org.example.MultiFileMetricExporter "${P_FILE_PATH}" "${C_FILE_PATH}" -o "${OUTPUT_FILE_PATH}"
echo "Export job done."

echo "Execution completed."
