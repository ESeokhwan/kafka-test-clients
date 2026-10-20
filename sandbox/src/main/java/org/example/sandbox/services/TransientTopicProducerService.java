package org.example.sandbox.services;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import moniq.MonitorLog;
import moniq.MonitorQueue;
import moniq.util.IMessageAdaptor;
import moniq.writer.MonitorLogWriter;
import org.apache.kafka.clients.producer.KafkaTransientProducer;
import org.apache.kafka.clients.producer.TransientCallback;
import org.apache.kafka.clients.producer.TransientProducerRecord;
import org.apache.kafka.clients.producer.TransientRecordMetadata;
import org.apache.kafka.clients.producer.TransientTopicHandle;
import org.example.core.AbstractService;
import org.example.core.util.TimeUtils;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TransientTopicProducerService extends AbstractService {

    private final String topicName;
    private final int roundCnt;
    private final boolean isSync;
    private final boolean ignoreResponse;
    private final boolean needFlush;
    private final boolean logEnabled;
    private final boolean msgTagged;

    private final IMessageAdaptor messageAdaptor;
    private final MonitorQueue monitoringQueue;
    private final MonitorLogWriter monitorLogWriter;

    private final KafkaTransientProducer<String, String> producer;
    private final boolean needToCleanupProducer;

    private final AtomicInteger curIdx = new AtomicInteger(0);

    private TransientTopicHandle topic = null;

    public TransientTopicProducerService(
            String brokers, String clientId, String topicName, int roundCnt,
            int interval, double intervalNoiseStddev, int intervalMaxAbsNoise, Random randomEngine, boolean isSync, boolean ignoreResponse, boolean needFlush,
            boolean logEnabled, boolean msgTagged, IMessageAdaptor messageAdaptor, MonitorQueue monitoringQueue, MonitorLogWriter monitorLogWriter
    ) {
        super(roundCnt, interval, intervalNoiseStddev, intervalMaxAbsNoise, randomEngine);
        this.topicName = topicName;
        this.roundCnt = roundCnt;
        this.isSync = isSync;
        this.ignoreResponse = ignoreResponse;
        this.needFlush = needFlush;
        this.logEnabled = logEnabled;
        this.msgTagged = msgTagged;
        this.messageAdaptor = messageAdaptor;
        this.monitoringQueue = monitoringQueue;
        this.monitorLogWriter = monitorLogWriter;

        Properties producerProps = createProducerConfig(brokers, clientId, isSync || !ignoreResponse);
        this.producer = new KafkaTransientProducer<>(producerProps);
        this.needToCleanupProducer = true;
    }

    public TransientTopicProducerService(
            KafkaTransientProducer<String, String> producer, String topicName, int roundCnt,
            int interval, double intervalNoiseStddev, int intervalMaxAbsNoise, Random randomEngine, boolean isSync, boolean ignoreResponse, boolean needFlush,
            boolean logEnabled, boolean msgTagged, IMessageAdaptor messageAdaptor, MonitorQueue monitoringQueue, MonitorLogWriter monitorLogWriter
    ) {
        super(roundCnt, interval, intervalNoiseStddev, intervalMaxAbsNoise, randomEngine);
        this.topicName = topicName;
        this.roundCnt = roundCnt;
        this.isSync = isSync;
        this.ignoreResponse = ignoreResponse;
        this.needFlush = needFlush;
        this.logEnabled = logEnabled;
        this.msgTagged = msgTagged;
        this.messageAdaptor = messageAdaptor;
        this.monitoringQueue = monitoringQueue;
        this.monitorLogWriter = monitorLogWriter;

        this.producer = producer;
        this.needToCleanupProducer = false;
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

    public void work() {
        String coreMessage = topicName + "_" + curIdx.getAndIncrement();
        if (msgTagged && logEnabled) coreMessage = "R" + coreMessage; // TODO: use a better tagging strategy
        String message = messageAdaptor.generate(coreMessage);

        logRequested(coreMessage);
        if (topic == null) topic = createNewTransientTopic(topicName);
        TransientProducerRecord<String, String> record = new TransientProducerRecord<>(topic, message);
        producer.send(record, new ProducerCallback(record));
        if (needFlush || isSync) producer.flush();

        if (isDone()) {
            producer.seal(topic);
            producer.flush();
        }
    }

    @Override
    public void close() {
        if (producer != null && needToCleanupProducer) producer.close();
    }

    private void logRequested(String coreMessage) {
        if (!logEnabled) return;
        long timestamp = TimeUtils.getCurrentTimeMillis();
        long timestampNano = TimeUtils.getCurrentTimeNanos();
        monitoringQueue.enqueue(new MonitorLog(
                "Producer", coreMessage, "REQUESTED", timestamp, timestampNano
        ));
        monitorLogWriter.notifyIfNeeded();
    }

    private void logCompleted(String coreMessage) {
        if (!logEnabled) return;
        long timestamp = TimeUtils.getCurrentTimeMillis();
        long timestampNano = TimeUtils.getCurrentTimeNanos();
        monitoringQueue.enqueue(new MonitorLog(
                "Producer", coreMessage, "RESPONDED", timestamp, timestampNano
        ));
        monitorLogWriter.notifyIfNeeded();
    }

    private TransientTopicHandle createNewTransientTopic(String topicName) {
        try {
            return producer.create(topicName).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            producer.close(Duration.ZERO);
            throw new IllegalStateException("interrupt during transient topic creation", e);
        } catch (ExecutionException e) {
            producer.close(Duration.ZERO);
            throw new IllegalStateException("transient topic creation failed", e.getCause());
        }
    }

    private class ProducerCallback implements TransientCallback {

        private final TransientProducerRecord<String, String> record;

        public ProducerCallback(TransientProducerRecord<String, String> record) {
            this.record = record;
        }

        @Override
        public void onCompletion(TransientRecordMetadata metadata, Exception exception) {
            if (!ignoreResponse) logCompleted(messageAdaptor.extractMessageId(record.value()));
        }
    }
}
