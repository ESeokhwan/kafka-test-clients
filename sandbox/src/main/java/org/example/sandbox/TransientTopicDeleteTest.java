package org.example.sandbox;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import moniq.MonitorLog;
import moniq.MonitorQueue;
import moniq.writer.MonitorLogWriter;
import moniq.writer.strategy.ScrapableWriteStrategy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.logging.log4j.ThreadContext;
import org.example.core.util.TimeUtils;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.Properties;

@Slf4j
public class TransientTopicDeleteTest implements Runnable {

    @Getter
    @Option(names = {"-b", "--brokers"}, required = true, description = "Kafka Brokers (comma-separated list)")
    private String brokers;

    @Getter
    @Option(names = {"-t", "--topic"}, required = true, description = "topic name to be deleted")
    private String topic;

    @Getter
    @Option(names = "--monitoring-batch-size", description = "Batch size for monitoring log writing. Default: 10,000,000")
    private int monitoringBatchSize = 10_000_000;

    private final MonitorQueue monitoringQueue = new MonitorQueue();
    private MonitorLogWriter monitorLogWriter;
    private Thread monitorLogWriterThread;

    public TransientTopicDeleteTest() {
        super();
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

    @Override
    public void run() {
        init();

        Properties props = createAdminClientConfig();
        try (AdminClient adminClient = KafkaAdminClient.create(props)) {
            addMonitorLog(topic, "REQUESTED");
            adminClient.deleteTransientTopics(List.of(topic)).all().get();
            addMonitorLog(topic, "RESPONDED");
        } catch (Exception e) {
            log.error("Failed to create AdminClient", e);
        }
        cleanupLogWriter();
    }

    private Properties createAdminClientConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.brokers);

        return props;
    }

    private void addMonitorLog(String topicName, String status) {
        long timestamp = TimeUtils.getCurrentTimeMillis();
        long timestampNano = TimeUtils.getCurrentTimeNanos();
        addMonitorLog(topicName, status, timestamp, timestampNano);
    }

    private void addMonitorLog(String topicName, String status, long timestamp, long timestampNano) {
        monitoringQueue.enqueue(new MonitorLog(
                "DELETE_TRANSIENT_TOPIC",
                topicName,
                status,
                timestamp,
                timestampNano
        ));
        monitorLogWriter.notifyIfNeeded();
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

        new CommandLine(new TransientTopicDeleteTest())
                .execute(args);
    }
}
