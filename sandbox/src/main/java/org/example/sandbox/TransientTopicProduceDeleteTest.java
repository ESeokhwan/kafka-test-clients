package org.example.sandbox;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import moniq.MonitorLog;
import moniq.MonitorQueue;
import moniq.writer.MonitorLogWriter;
import moniq.writer.strategy.ScrapableWriteStrategy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaTransientTopicProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.ThreadContext;
import org.example.util.NoiseUtils;
import org.example.util.TimeUtils;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TransientTopicProduceDeleteTest implements Runnable {

    @Getter
    @Option(names = {"-b", "--brokers"}, required = true, description = "Kafka Brokers (comma-separated list)")
    private String brokers;

    @Getter
    @Option(names = {"-p", "--prefix"}, required = true, description = "Prefix of topic name and messages to be created")
    private String prefix;

    @Getter
    @Option(names = {"-i", "--interval"}, description = "Interval between each round in milli seconds. Default: 1,000")
    private int interval = 1000;

    @Getter
    @Option(names = "--interval-noise-stddev", description = "Noise standard deviation of interval between each round in milli seconds. Default: 0")
    private double noiseStddev = 0;

    @Getter
    @Option(names = {"-n", "--group-count"}, description = "Number of groups. Default: 10")
    private int groupCount = 10;

    @Getter
    @Option(names = "--per-group-count", description = "Number of messages in each group. Default: 1")
    private int perGroupCount = 1;

    @Getter
    @Option(names = {"-a", "--is-async"}, description = "If true, topic creation will be done asynchronously. Default: false")
    private boolean isAsync = false;

    @Getter
    @Option(names = "--monitoring-batch-size", description = "Batch size for monitoring log writing. Default: 10,000,000")
    private int monitoringBatchSize = 10_000_000;

    private final MonitorQueue monitoringQueue = new MonitorQueue();
    private MonitorLogWriter monitorLogWriter;
    private Thread monitorLogWriterThread;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public TransientTopicProduceDeleteTest() {
        super();
    }

    @Override
    public void run() {
        init();

        Random randomEngine = new Random();
        List<Integer> intervalNoises = NoiseUtils.generateNoiseList(
                noiseStddev,
                interval / 2,
                Math.min(perGroupCount * groupCount, NoiseUtils.MAX_NOISE_LIST_LENGTH),
                randomEngine
        );

        Properties props = createProducerConfig();
        for (int i = 0; i < groupCount; i++) {
            String topicName = prefix + "_" + String.valueOf(i);
            try (Producer<String, String> producer = new KafkaTransientTopicProducer<>(props)) {
                for (int j = 0; j < perGroupCount; j++) {
                    long startTimestamp = TimeUtils.getAccurateCurrentTimeMillis();
                    String messageId = topicName + "_" + j;
                    ProducerRecord<String, String> record = new ProducerRecord<>(topicName, messageId);
                    addMonitorLog("PRODUCE", messageId, "REQUESTED");
                    if (isAsync) producer.send(record, new BasicProducerCallback(record));
                    else producer.send(record, new BasicProducerCallback(record)).get();

                    long elapsedTimeMs = TimeUtils.getAccurateCurrentTimeMillis() - startTimestamp;
                    long curInterval = interval + intervalNoises.get((i * perGroupCount + j) % intervalNoises.size());
                    try {
                        Thread.sleep(Math.max(curInterval - (int) elapsedTimeMs, 0));
                    } catch (InterruptedException e) {
                        log.error("Thread interrupted during sleep", e);
                        Thread.currentThread().interrupt();
                    }
                }
            } catch (Exception e) {
                log.error("Failed to create AdminClient", e);
            }
        }
        cleanupLogWriter();
    }

    private void init() {
        monitorLogWriter = new MonitorLogWriter(
                monitoringQueue,
                new ScrapableWriteStrategy(System.out),
                monitoringBatchSize
        );
        monitorLogWriterThread = new Thread(monitorLogWriter);
        monitorLogWriterThread.start();
    }

    private Properties createProducerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("client.id", "transient-topic-producer-test");
        props.put("batch.size", "0");

        return props;
    }

    public class BasicProducerCallback implements Callback {

        private final ProducerRecord<String, String> record;

        public BasicProducerCallback(ProducerRecord<String, String> record) {
            this.record = record;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                exception.printStackTrace();
                return;
            }

            String messageId = record.value();
            addMonitorLog("PRODUCE", messageId, "RESPONDED");
            monitorLogWriter.notifyIfNeeded();

            String topic = record.topic();
            scheduler.schedule(() -> {
                Properties props = createAdminClientConfig();
                try (AdminClient adminClient = KafkaAdminClient.create(props)) {
                    addMonitorLog("TOPIC_DELETE", topic, "REQUESTED");
                    adminClient.deleteTransientTopics(List.of(topic)).all().get();
                    addMonitorLog("TOPIC_DELETE", topic, "RESPONDED");
                } catch (Exception e) {
                    log.error("Failed to create AdminClient", e);
                }
            }, 1, TimeUnit.SECONDS);
        }

        private Properties createAdminClientConfig() {
            Properties props = new Properties();
            props.put("bootstrap.servers", brokers);

            return props;
        }
    }

    private void addMonitorLog(String type, String messageId, String status, long timestamp, long timestampNano) {
        monitoringQueue.enqueue(new MonitorLog(
                type,
                messageId,
                status,
                timestamp,
                timestampNano
        ));
        monitorLogWriter.notifyIfNeeded();
    }

    private void addMonitorLog(String type, String messageId, String state) {
        long timestamp = TimeUtils.getCurrentTimeMillis();
        long timestampNano = TimeUtils.getCurrentTimeNanos();
        addMonitorLog(type, messageId, state, timestamp, timestampNano);
    }

    private void cleanupLogWriter() {
        monitorLogWriter.gracefulShutdown();
        monitorLogWriter.syncedNotify();
        try {
            monitorLogWriterThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
        String pid = rt.getName();
        ThreadContext.put("PID", pid);

        new CommandLine(new TransientTopicProduceDeleteTest())
                .execute(args);
    }
}
