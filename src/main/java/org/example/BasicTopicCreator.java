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
import org.example.util.NoiseUtils;
import org.example.util.TimeUtils;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

@Slf4j
public class BasicTopicCreator implements Runnable {

    @Getter
    @Option(names = {"-b", "--brokers"}, required = true, description = "Kafka Brokers (comma-separated list)")
    private String brokers;

    @Getter
    @Option(names = {"-p", "--topic-prefix"}, required = true, description = "Prefix of topic names to be created")
    private String topicPrefix;

    @Getter
    @Option(names = {"--partition-count"}, description = "Number of partitions for each topic. Default: 1")
    private int partitionCount = 1;

    @Getter
    @Option(names = {"--replication-factor"}, description = "Replication factor for each topic. Default: 1")
    private short replicationFactor = 1;

    @Getter
    @Option(names = {"-s", "--start-index"}, description = "Start index of topic names. It will be used with topicPrefix. Default: 0")
    private int startIndex = 0;

    @Getter
    @Option(names = {"-n", "--round-count"}, description = "Number of rounds. Default: 10")
    private int roundCount = 10;

    @Getter
    @Option(names = {"-i", "--interval"}, description = "Interval between each round in milli seconds. Default: 1,000")
    private int interval = 1000;

    @Getter
    @Option(names = "--interval-noise-stddev", description = "Noise standard deviation of interval between each round in milli seconds. Default: 0")
    private double noiseStddev = 0;

    @Getter
    @Option(names = "--per-round-count", description = "Number of topics in each round. Default: 1")
    private int perRoundCount = 1;

    @Getter
    @Option(names = {"-B", "--use-batch"}, description = "If true, topic creation will be batched for each round. Default: false")
    private boolean useBatch = false;

    @Getter
    @Option(names = {"-a", "--is-async"}, description = "If true, topic creation will be done asynchronously. Default: false")
    private boolean isAsync = false;

    @Getter
    @Option(names = "--monitoring-batch-size", description = "Batch size for monitoring log writing. Default: 10,000,000")
    private int monitoringBatchSize = 10_000_000;

    private final MonitorQueue monitoringQueue = new MonitorQueue();
    private MonitorLogWriter monitorLogWriter;
    private Thread monitorLogWriterThread;

    public BasicTopicCreator() {
        super();
    }

    @Override
    public void run() {
        init();

        Random randomEngine = new Random();
        List<Integer> intervalNoises = NoiseUtils.generateNoiseList(
                noiseStddev,
                interval / 2,
                Math.min(roundCount, NoiseUtils.MAX_NOISE_LIST_LENGTH),
                randomEngine
        );

        Properties props = createAdminClientConfig();
        try (AdminClient adminClient = KafkaAdminClient.create(props)) {
            int totalCnt = 0;
            for (int i = 0; i < roundCount; i++) {
                long startTimestamp = TimeUtils.getAccurateCurrentTimeMillis();
                List<String> topicNames = new ArrayList<>();
                for (int j = 0; j < perRoundCount; j++) {
                    topicNames.add(topicPrefix + (startIndex + totalCnt));
                    totalCnt += 1;
                }
                doCreateTopics(adminClient, topicNames);

                long elapsedTimeMs = TimeUtils.getAccurateCurrentTimeMillis() - startTimestamp;
                long curInterval = interval + intervalNoises.get(i % intervalNoises.size());
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

    private Properties createAdminClientConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.brokers);

        return props;
    }

    private void doCreateTopics(AdminClient adminClient, List<String> topicNames) {
        if (useBatch) {
            doCreateTopicBatch(adminClient, topicNames);
            return;
        }

        for (String topicName: topicNames) {
            doCreateTopic(adminClient, topicName);
        }
    }

    private void doCreateTopic(AdminClient adminClient, String topicName) {
        addMonitorLog(topicName, "REQUESTED");
        KafkaFuture<Void> future = adminClient.createTopics(
                List.of(new NewTopic(topicName, partitionCount, replicationFactor))
        ).all();
        if (isAsync) return;

        try {
            future.get(); // Wait for the creation to complete if not async
            addMonitorLog(topicName, "RESPONDED");
        } catch (InterruptedException | ExecutionException e) {
            addMonitorLog(topicName, "FAILED");
        }
    }

    private void doCreateTopicBatch(AdminClient adminClient, List<String> topicNames) {
        long stTimestamp = TimeUtils.getCurrentTimeMillis();
        long stTimestampNano = TimeUtils.getCurrentTimeNanos();

        List<NewTopic> topics = topicNames.stream().map(
                t -> new NewTopic(t, partitionCount, replicationFactor)
        ).toList();
        KafkaFuture<Void> future = adminClient.createTopics(topics).all();
        if (isAsync) {
            for (String topicName: topicNames) {
                addMonitorLog(topicName, "REQUESTED", stTimestamp, stTimestampNano);
            }
            return;
        }

        try {
            future.get(); // Wait for the creation to complete if not async
            long enTimestamp = TimeUtils.getCurrentTimeMillis();
            long enTimestampNano = TimeUtils.getCurrentTimeNanos();
            for (String topicName: topicNames) {
                addMonitorLog(topicName, "REQUESTED", stTimestamp, stTimestampNano);
                addMonitorLog(topicName, "RESPONDED", enTimestamp, enTimestampNano);
            }
        } catch (InterruptedException | ExecutionException e) {
            long enTimestamp = TimeUtils.getCurrentTimeMillis();
            long enTimestampNano = TimeUtils.getCurrentTimeNanos();
            for (String topicName: topicNames) {
                addMonitorLog(topicName, "REQUESTED", stTimestamp, stTimestampNano);
                addMonitorLog(topicName, "FAILED", enTimestamp, enTimestampNano);
            }
        }
    }

    private void addMonitorLog(String topicName, String status, long timestamp, long timestampNano) {
        monitoringQueue.enqueue(new MonitorLog(
                "CREATE_TOPIC",
                topicName,
                status,
                timestamp,
                timestampNano
        ));
        monitorLogWriter.notifyIfNeeded();
    }

    private void addMonitorLog(String topicName, String state) {
        long timestamp = TimeUtils.getCurrentTimeMillis();
        long timestampNano = TimeUtils.getCurrentTimeNanos();
        addMonitorLog(topicName, state, timestamp, timestampNano);
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

        new CommandLine(new BasicTopicCreator())
                .execute(args);
    }
}
