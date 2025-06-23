package org.example;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import moniq.MonitorLog;
import moniq.MonitorQueue;
import moniq.writer.MonitorLogWriter;
import moniq.writer.strategy.ScrapableWriteStrategy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.ThreadContext;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class RegularlyTopicDeletionWithBatch implements Runnable {

    @Getter
    @Parameters(index = "0", description = "Kafka Brokers")
    private String brokers;

    @Getter
    @Parameters(index = "1", description = "Topic Prefix")
    private String topicPrefix;

    @Getter
    @Option(names = {"-s", "--start-index"}, description = "Start index of topic names. It will be used with topicPrefix. Default: 0")
    private int startIndex = 0;

    @Getter
    @Option(names = {"-n", "--round-count"}, description = "Number of rounds. Default: 10")
    private int roundCount = 10;

    @Getter
    @Option(names = {"-i", "--interval"}, description = "Interval between each round in milli seconds. Default: 1000")
    private int interval = 1000;

    @Getter
    @Option(names = {"-N", "--per-round-count"}, description = "Number of topics in each round. It will be used with topicPrefix and startIndex. Default: 10")
    private int perRoundCount = 10;

    @Getter
    @Option(names = {"-B", "--use-batch"}, description = "If true, topic deletion will be batched for each round. Default: true")
    private boolean useBatch = true;

    @Getter
    @Option(names = {"-a", "--is-async"}, description = "If true, topic creation will be done asynchronously. Default: false")
    private boolean isAsync = false;

    private final MonitorQueue monitoringQueue = new MonitorQueue();
    private MonitorLogWriter monitorLogWriter;
    private Thread monitorLogWriterThread;

    public RegularlyTopicDeletionWithBatch() {
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
            int totalCnt = 0;
            for (int i = 0; i < roundCount; i++) {
                long startTimestamp = System.currentTimeMillis();
                List<String> topicNames = new ArrayList<>();
                for (int j = 0; j < perRoundCount; j++) {
                    topicNames.add(topicPrefix + (startIndex + totalCnt));
                    totalCnt += 1;
                }
                doDeleteTopics(adminClient, topicNames);

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

    private void doDeleteTopics(AdminClient adminClient, List<String> topicNames) {
        if (useBatch) {
            doDeleteTopicBatch(adminClient, topicNames);
            return;
        }

        for (String topicName: topicNames) {
            doDeleteTopic(adminClient, topicName);
        }
    }

    private void doDeleteTopic(AdminClient adminClient, String topicName) {
        addMonitorLog(topicName, "REQUESTED");

        KafkaFuture<Void> future = adminClient.deleteTopics(List.of(topicName)).all();
        if (isAsync) return;

        try {
            future.get(); // Wait for the deletion to complete if not async
            addMonitorLog(topicName, "RESPONDED");
        } catch (InterruptedException | ExecutionException e) {
            addMonitorLog(topicName, "FAILED");
        }
    }

    private void doDeleteTopicBatch(AdminClient adminClient, List<String> topicNames) {
        long stTimestamp = System.currentTimeMillis();
        long stTimestampNano = System.nanoTime();

        KafkaFuture<Void> future = adminClient.deleteTopics(topicNames).all();
        if (isAsync) {
            for (String topicName: topicNames) {
                addMonitorLog(topicName, "REQUESTED", stTimestamp, stTimestampNano);
            }
            return;
        }

        try {
            future.get(); // Wait for the deletion to complete if not async
            long enTimestamp = System.currentTimeMillis();
            long enTimestampNano = System.nanoTime();
            for (String topicName: topicNames) {
                addMonitorLog(topicName, "REQUESTED", stTimestamp, stTimestampNano);
                addMonitorLog(topicName, "RESPONDED", enTimestamp, enTimestampNano);
            }
        } catch (InterruptedException | ExecutionException e) {
            long enTimestamp = System.currentTimeMillis();
            long enTimestampNano = System.nanoTime();
            for (String topicName: topicNames) {
                addMonitorLog(topicName, "REQUESTED", stTimestamp, stTimestampNano);
                addMonitorLog(topicName, "FAILED", enTimestamp, enTimestampNano);
            }
        }
    }

    private void addMonitorLog(String topicName, String status, long timestamp, long timestampNano) {
        monitoringQueue.enqueue(new MonitorLog(
                "DELETE_TOPIC",
                topicName,
                status,
                timestamp,
                timestampNano
        ));
        monitorLogWriter.notifyIfNeeded();
    }

    private void addMonitorLog(String topicName, String status) {
        long timestamp = System.currentTimeMillis();
        long timestampNano = System.nanoTime();
        monitoringQueue.enqueue(new MonitorLog(
                "DELETE_TOPIC",
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

        new CommandLine(new RegularlyTopicDeletionWithBatch())
                .execute(args);
    }
}
