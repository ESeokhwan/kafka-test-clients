#!/bin/bash

# 1차 Java 명령어 실행
echo "Running Java Kafka producer..."
java -cp /home/huan99/kafka_test_clients/build/libs/kafka_test_clients-1.0-SNAPSHOT-all.jar org.example.BasicProducerWithMonitor localhost:9092 test_topic_ms_10000 -s 10000 -n 400000 -m output/cur/producer_monitor_cold.csv

# Produce 완료 메시지
echo "Producer job done."

# 잠시 정지
echo "Sleep a 5s..."
sleep 5

echo "Export log to metric file..."
java -cp /home/huan99/kafka_test_clients/build/libs/kafka_test_clients-1.0-SNAPSHOT-all.jar org.example.NaiveProducerMetricExporter /home/huan99/kafka_test_clients/output/cur/producer_monitor_cold.csv -o /home/huan99/kafka_test_clients/output/cur/exported_metric_cold.csv

# 2차 완료 메시지
echo "Export job done."
