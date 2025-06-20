package org.example;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import moniq.MonitorLog;
import moniq.MonitorQueue;
import moniq.writer.MonitorLogWriter;
import moniq.writer.strategy.ScrapableWriteStrategy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.ThreadContext;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class RegularlyTopicCreationTest implements Runnable {

    @Getter
    @Parameters(index = "0", description = "Kafka Brokers")
    private String brokers;

    @Getter
    @Parameters(index = "1", description = "Topic Prefix")
    private String topicPrefix;

    @Getter
    @Option(names = {"-p", "--partition-count"}, description = "Number of partitions for each topic. Default: 1")
    private int partitionCount = 1;

    @Getter
    @Option(names = {"-r", "--replication-factor"}, description = "Replication factor for each topic. Default: 1")
    private short replicationFactor = 1;

    @Getter
    @Option(names = {"-s", "--start-index"}, description = "Start index of topic names. It will be used with topicPrefix. Default: 0")
    private int startIndex = 0;

    @Getter
    @Option(names = {"-c", "--count"}, description = "Number of topics. It will be used with topicPrefix and startIndex. Default: 10")
    private int count = 10;

    @Getter
    @Option(names = {"-i", "--interval"}, description = "Interval of topic deletion in milli seconds. Default: 1000")
    private int interval = 1000;

    @Getter
    @Option(names = {"-a", "--is-async"}, description = "If true, topic creation will be done asynchronously. Default: false")
    private boolean isAsync = false;

    private final MonitorQueue monitoringQueue = new MonitorQueue();
    private MonitorLogWriter monitorLogWriter;
    private Thread monitorLogWriterThread;

    public RegularlyTopicCreationTest() {
        super();
    }

    @Override
    public void run() {
        monitorLogWriter = new MonitorLogWriter(
                monitoringQueue,
                new ScrapableWriteStrategy(System.out),
                1000
        );
        monitorLogWriterThread = new Thread(monitorLogWriter);
        monitorLogWriterThread.start();

        Properties props = createAdminClientConfig();
        try (AdminClient adminClient = KafkaAdminClient.create(props)) {
            for (int i = 0; i < count; i++) {
                long startTimestamp = System.currentTimeMillis();
                String topicName = topicPrefix + (startIndex + i);
                doCreateTopic(adminClient, topicName);
                try {
                    long elapsedTimeMs = System.currentTimeMillis() - startTimestamp;
                    Thread.sleep(Math.max(interval - (int) elapsedTimeMs, 0));
                } catch (InterruptedException e) {
                    log.error("Thread interrupted during sleep", e);
                    Thread.currentThread().interrupt();
                }
            }
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

    private void doCreateTopic(AdminClient adminClient, String topicName) {
        addMonitorLog(topicName, "REQUESTED");
        KafkaFuture<Void> future = adminClient.createTopics(
                List.of(new NewTopic(topicName, partitionCount, replicationFactor))
        ).all();
        if (!isAsync) {
            try {
                future.get(); // Wait for the deletion to complete if not async
                addMonitorLog(topicName, "RESPONDED");
            } catch (InterruptedException | ExecutionException e) {
                addMonitorLog(topicName, "FAILED");
            }
        }
    }

    private void addMonitorLog(String topicName, String state) {
        long timestamp = System.currentTimeMillis();
        long timestampNano = System.nanoTime();
        monitoringQueue.enqueue(new MonitorLog(
                "CREATE_TOPIC",
                topicName,
                state,
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

        new CommandLine(new RegularlyTopicCreationTest())
                .execute(args);
    }
}
