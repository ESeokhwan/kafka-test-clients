#!/bin/bash

if [ $# -lt 1 ];
then
	echo "USAGE: $0 command [additional-arguments]"
	exit 1
fi
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
BASE_DIR="$SCRIPT_DIR/.."

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

# Set the JAR file path
JAR_FILE=$BASE_DIR/build/libs/kafka_test_clients-1.0-SNAPSHOT-all.jar
if [ ! -f "$JAR_FILE" ]; then
    echo "JAR file $JAR_FILE not found!"
    exit 1
fi

COMMAND=$1
shift

exec "$JAVA" -cp "$JAR_FILE" org.example."$COMMAND" "$@"
