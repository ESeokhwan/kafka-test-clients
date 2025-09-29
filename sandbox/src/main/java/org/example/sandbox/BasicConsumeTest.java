package org.example.sandbox;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import moniq.MonitorLog;
import moniq.MonitorQueue;
import moniq.writer.MonitorLogWriter;
import moniq.writer.strategy.ScrapableWriteStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.ThreadContext;
import org.example.util.NoiseUtils;
import org.example.util.TimeUtils;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

@Slf4j
public class BasicConsumeTest implements Runnable {

    @Getter
    @Option(names = {"-b", "--brokers"}, required = true, description = "Kafka Brokers (comma-separated list)")
    private String brokers;

    @Getter
    @Option(names = {"-t", "--topic"}, required = true, description = "topic name to consume")
    private String topic;

    @Getter
    @Option(names = {"-p", "--partition"}, required = true, description = "partition number to consume")
    private int partition;

    @Getter
    @Option(names = {"-r", "--runtime"}, description = "Total runtime(sec) of this consumer. It will run eternally, when this value is set 0. Default: 1800 sec")
    private long runtime = 30 * 60;

    @Getter
    @Option(names = "--monitoring-batch-size", description = "Batch size for monitoring log writing. Default: 10,000,000")
    private int monitoringBatchSize = 10_000_000;

    private final MonitorQueue monitoringQueue = new MonitorQueue();
    private MonitorLogWriter monitorLogWriter;
    private Thread monitorLogWriterThread;

    public BasicConsumeTest() {
        super();
    }

    @Override
    public void run() {
        init();

        Properties props = createConsumerConfig();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            System.out.println("topic: " + topic + ", partition: " + partition);
            log.info("topic: " + topic + ", partition: " + partition);
            consumer.assign(Arrays.asList(new TopicPartition(topic, partition)));

            long expiredTime = System.nanoTime() + runtime * 1000 * 1000 * 1000;
            while (runtime == 0 || System.nanoTime() < expiredTime) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
                for (var record: records) addMonitorLog(record.value(), "CONSUMED");
            }
        } catch (Exception e) {
            log.error("Failed to create Consumer", e);
        } finally {
            cleanupLogWriter();
        }
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

    private Properties createConsumerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.brokers);
        props.put("group.id", UUID.randomUUID().toString());
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("fetch.min.bytes", 1);

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return props;
    }

    private void addMonitorLog(String messageId, String status, long timestamp, long timestampNano) {
        monitoringQueue.enqueue(new MonitorLog(
                "Consume",
                messageId,
                status,
                timestamp,
                timestampNano
        ));
        monitorLogWriter.notifyIfNeeded();
    }

    private void addMonitorLog(String messageId, String state) {
        long timestamp = TimeUtils.getCurrentTimeMillis();
        long timestampNano = TimeUtils.getCurrentTimeNanos();
        addMonitorLog(messageId, state, timestamp, timestampNano);
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

        new CommandLine(new BasicConsumeTest())
                .execute(args);
    }
}
