#!/bin/bash

# 1차 Java 명령어 실행
echo "Running Java Kafka consumer..."
java -cp /home/huan99/kafka_test_clients/build/libs/kafka_test_clients-1.0-SNAPSHOT-all.jar org.example.EarliestConsumerWithMonitor localhost:9092 old_topic_ms_10000 -s 10000 -n 30000 -m /home/huan99/kafka_test_clients/output/cur/consumer_monitor_earliest_hot_1.csv

# 1차 완료 메시지
echo "First execution completed."

# 잠시 정지
echo "Sleep a 500 ms"
usleep 500000

# 2차 Java 명령어 실행
echo "Running Java Kafka consumer..."
java -cp /home/huan99/kafka_test_clients/build/libs/kafka_test_clients-1.0-SNAPSHOT-all.jar org.example.EarliestConsumerWithMonitor localhost:9092 old_topic_ms_10000 -s 10000 -n 30000 -m /home/huan99/kafka_test_clients/output/cur/consumer_monitor_earliest_hot_2.csv

# 2차 완료 메시지
echo "Second execution completed."
