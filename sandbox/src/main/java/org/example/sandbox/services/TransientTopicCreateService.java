package org.example.sandbox.services;

import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import moniq.MonitorLog;
import moniq.MonitorQueue;
import moniq.writer.MonitorLogWriter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaTransientTopicProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.example.core.AbstractService;
import org.example.core.util.TimeUtils;

@Slf4j
public class TransientTopicCreateService extends AbstractService {

    private final String topicPrefix;
    private final int startIndex;
    private final int roundCnt;
    private final int perRoundCnt;
    private final boolean isSync;
    private final boolean ignoreResponse;
    private final boolean logEnabled;

    private final MonitorQueue monitoringQueue;
    private final MonitorLogWriter monitorLogWriter;

    private final Producer<String, String> producer;
    private final boolean needToCleanupClient;

    private final AtomicInteger curIdx = new AtomicInteger(0);

    public TransientTopicCreateService(
            String brokers, String clientId, String topicPrefix, int startIndex, int roundCnt, int perRoundCnt,
            int interval, double intervalNoiseStddev, int intervalMaxAbsNoise, Random randomEngine,
            boolean isSync, boolean ignoreResponse, boolean logEnabled, MonitorQueue monitoringQueue, MonitorLogWriter monitorLogWriter
    ) {
        super(roundCnt, interval, intervalNoiseStddev, intervalMaxAbsNoise, randomEngine);
        this.topicPrefix = topicPrefix;
        this.startIndex = startIndex;
        this.roundCnt = roundCnt;
        this.perRoundCnt = perRoundCnt;
        this.isSync = isSync;
        this.ignoreResponse = ignoreResponse;
        this.logEnabled = logEnabled;
        this.monitoringQueue = monitoringQueue;
        this.monitorLogWriter = monitorLogWriter;

        Properties producerProps = createProducerConfig(brokers, clientId, isSync || !ignoreResponse);
        this.producer = new KafkaTransientTopicProducer<>(producerProps);
        this.needToCleanupClient = true;
    }

    public TransientTopicCreateService(
            Producer<String, String> producer, String topicPrefix, int startIndex, int roundCnt, int perRoundCnt,
            int interval, double intervalNoiseStddev, int intervalMaxAbsNoise, Random randomEngine,
            boolean isSync, boolean ignoreResponse, boolean logEnabled, MonitorQueue monitoringQueue, MonitorLogWriter monitorLogWriter
    ) {
        super(roundCnt, interval, intervalNoiseStddev, intervalMaxAbsNoise, randomEngine);
        this.topicPrefix = topicPrefix;
        this.startIndex = startIndex;
        this.roundCnt = roundCnt;
        this.perRoundCnt = perRoundCnt;
        this.isSync = isSync;
        this.ignoreResponse = ignoreResponse;
        this.logEnabled = logEnabled;
        this.monitoringQueue = monitoringQueue;
        this.monitorLogWriter = monitorLogWriter;

        this.producer = producer;
        this.needToCleanupClient = false;
    }

    public static Properties createProducerConfig(String brokers, String clientId, boolean needAcks) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("client.id", clientId);
        props.put("batch.size", "1");
        props.put("linger.ms", "0");
        props.put("acks", needAcks ? "all" : "0");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    @Override
    public boolean isDone() {
        return curIdx.get() >= roundCnt;
    }

    @Override
    public void work() {
        int curTopicIdx = startIndex + curIdx.getAndIncrement() * perRoundCnt;
        List<String> topics = new ArrayList<>();
        for (int i = 0; i < perRoundCnt; i++) topics.add(topicPrefix + "_" + (curTopicIdx + i));
        doCreateTopics(topics);
    }

    @Override
    public void close() {
        if (producer != null && needToCleanupClient) producer.close();
    }

    private void doCreateTopics(List<String> topicNames) {
        try {
            for (String topicName : topicNames) {
                doCreateTopic(topicName);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void doCreateTopic(String topicName) throws InterruptedException {
        appendMonitorLog(topicName, "REQUESTED");

        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, topicName);
        producer.send(record, new CreatorProducerCallback(record));
        if (isSync) producer.flush();
    }

    private void appendMonitorLog(String topic, String status, long timestamp, long timestampNano) {
        if (!logEnabled) return;
        monitoringQueue.enqueue(new MonitorLog(
                "Creator", topic, status, timestamp, timestampNano
        ));
        monitorLogWriter.notifyIfNeeded();
    }

    private void appendMonitorLog(String topic, String status) {
        if (!logEnabled) return;
        long timestamp = TimeUtils.getCurrentTimeMillis();
        long timestampNano = TimeUtils.getCurrentTimeNanos();
        appendMonitorLog(topic, status, timestamp, timestampNano);
    }

    private class CreatorProducerCallback implements Callback {

        private final ProducerRecord<String, String> record;

        public CreatorProducerCallback(ProducerRecord<String, String> record) {
            this.record = record;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (!ignoreResponse) appendMonitorLog(record.value(), "RESPONDED", TimeUtils.getCurrentTimeMillis(), TimeUtils.getCurrentTimeNanos());
        }
    }
}
