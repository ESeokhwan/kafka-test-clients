package org.example.core.producer;

import lombok.extern.slf4j.Slf4j;
import moniq.MonitorLog;
import moniq.MonitorQueue;
import moniq.util.IMessageAdaptor;
import moniq.writer.MonitorLogWriter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.example.core.IService;
import org.example.core.util.NoiseUtils;
import org.example.core.util.Noises;
import org.example.core.util.TimeUtils;

import java.util.Properties;
import java.util.Random;

@Slf4j
public class ProducerService implements IService {

    private final String topicName;
    private final int msgCnt;
    private final int interval;
    private final boolean isSync;
    private final boolean needFlush;
    private final boolean logEnabled;
    private final boolean msgTagged;

    private final IMessageAdaptor messageAdaptor;
    private final MonitorQueue monitoringQueue;
    private final MonitorLogWriter monitorLogWriter;

    private int curIdx = 0;
    private final Noises noises;

    private final Producer<String, String> producer;
    private final boolean needToCleanupProducer;

    public ProducerService(
            String brokers, String clientId, String topicName, int msgCnt,
            int interval, double intervalNoiseStddev, int intervalMaxAbsNoise, Random randomEngine, boolean isSync, boolean needFlush,
            boolean logEnabled, boolean msgTagged, IMessageAdaptor messageAdaptor, MonitorQueue monitoringQueue, MonitorLogWriter monitorLogWriter
    ) {
        this.topicName = topicName;
        this.msgCnt = msgCnt;
        this.interval = interval;
        this.isSync = isSync;
        this.needFlush = needFlush;
        this.logEnabled = logEnabled;
        this.msgTagged = msgTagged;
        this.messageAdaptor = messageAdaptor;
        this.monitoringQueue = monitoringQueue;
        this.monitorLogWriter = monitorLogWriter;

        this.noises = NoiseUtils.generateNoises(
                intervalNoiseStddev, intervalMaxAbsNoise,
                Math.min(msgCnt, NoiseUtils.MAX_NOISE_LIST_LENGTH), randomEngine
        );
        Properties producerProps = createProducerConfig(brokers, clientId, isSync);
        this.producer = new KafkaProducer<>(producerProps);
        this.needToCleanupProducer = true;
    }

    public ProducerService(
            Producer<String, String> producer, String topicName, int msgCnt,
            int interval, double intervalNoiseStddev, int intervalMaxAbsNoise, Random randomEngine, boolean isSync, boolean needFlush,
            boolean logEnabled, boolean msgTagged, IMessageAdaptor messageAdaptor, MonitorQueue monitoringQueue, MonitorLogWriter monitorLogWriter
    ) {
        this.topicName = topicName;
        this.msgCnt = msgCnt;
        this.interval = interval;
        this.isSync = isSync;
        this.needFlush = needFlush;
        this.logEnabled = logEnabled;
        this.msgTagged = msgTagged;
        this.messageAdaptor = messageAdaptor;
        this.monitoringQueue = monitoringQueue;
        this.monitorLogWriter = monitorLogWriter;

        this.noises = NoiseUtils.generateNoises(
                intervalNoiseStddev, intervalMaxAbsNoise,
                Math.min(msgCnt, NoiseUtils.MAX_NOISE_LIST_LENGTH), randomEngine
        );
        this.producer = producer;
        this.needToCleanupProducer = false;
    }

    public static Properties createProducerConfig(String brokers, String clientId, boolean isSync) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("client.id", clientId);
        props.put("batch.size", "1");
        props.put("linger.ms", "0");
        props.put("acks", isSync ? "all" : "0");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public int curInterval() {
        int curNoise = noises.next();
        if (curIdx <= 0) return Math.abs(curNoise);
        return interval + curNoise;
    }

    public boolean hasMore() {
        return curIdx < msgCnt;
    }

    public void work() {
        String coreMessage = topicName + "_" + curIdx;
        if (msgTagged) coreMessage = "R" + coreMessage; // TODO: use a better tagging strategy
        String message = messageAdaptor.generate(coreMessage);

        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, message);
        if (logEnabled) logRequested(coreMessage);
        producer.send(record, new ProducerCallback(record));
        if (needFlush || isSync) producer.flush();
        curIdx += 1;
    }

    @Override
    public void close() {
        if (producer != null && needToCleanupProducer) producer.close();
    }

    private void logRequested(String coreMessage) {
        long timestamp = TimeUtils.getCurrentTimeMillis();
        long timestampNano = TimeUtils.getCurrentTimeNanos();
        monitoringQueue.enqueue(new MonitorLog(
                "Producer", coreMessage, "REQUESTED", timestamp, timestampNano
        ));
        monitorLogWriter.notifyIfNeeded();
    }

    private void logCompleted(String coreMessage) {
        long timestamp = TimeUtils.getCurrentTimeMillis();
        long timestampNano = TimeUtils.getCurrentTimeNanos();
        monitoringQueue.enqueue(new MonitorLog(
                "Producer", coreMessage, "RESPONDED", timestamp, timestampNano
        ));
        monitorLogWriter.notifyIfNeeded();
    }

    private class ProducerCallback implements Callback {

        private final ProducerRecord<String, String> record;

        public ProducerCallback(ProducerRecord<String, String> record) {
            this.record = record;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (logEnabled) logCompleted(messageAdaptor.extractMessageId(record.value()));
        }
    }
}
