#!/bin/bash

if [ $# -lt 2 ];
then
	echo "USAGE: $0 <module> <command> [additional-arguments]"
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

MODULE=$1
COMMAND=$2
shift 2

# Set the JAR file path
JAR_FILE="$BASE_DIR/build/libs/$MODULE-1.0-SNAPSHOT-all.jar"
TARGET_CLASS="org.example.$MODULE.$COMMAND"
if [ ! -f "$JAR_FILE" ]; then
    echo "JAR file $JAR_FILE not found!"
    echo "Please make sure you have built the '$MODULE' module first (e.g., ./gradlew :$MODULE:build)"
    exit 1
fi

echo "====================================================="
echo "Module      : $MODULE"
echo "Command     : $COMMAND"
echo "JAR         : $JAR_FILE"
echo "Main Class  : $TARGET_CLASS"
# shellcheck disable=SC2145
echo "Arguments   : $@"
echo "====================================================="

exec "$JAVA" -cp "$JAR_FILE" "$TARGET_CLASS" "$@"
