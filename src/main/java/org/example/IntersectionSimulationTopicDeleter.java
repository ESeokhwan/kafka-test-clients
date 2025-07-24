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
public class IntersectionSimulationTopicDeleter implements Runnable {

    @Getter
    @Option(names = {"-b", "--brokers"}, required = true, description = "Kafka Brokers (comma-separated list)")
    private String brokers;

    @Getter
    @Option(names = {"-p", "--topic-prefix"}, required = true, description = "Prefix of topic names to be deleted")
    private String topicPrefix;

    @Getter
    @Option(names = {"-s", "--start-index"}, description = "Start index of topic names. It will be used with topicPrefix. Default: 0")
    private int startIndex = 0;

    @Getter
    @Option(names = {"-n", "--round-count"}, description = "Number of rounds. Default: 10")
    private int roundCount = 10;

    @Getter
    @Option(names = "--monitoring-batch-size", description = "Batch size for monitoring log writing. Default: 10,000,000")
    private int monitoringBatchSize = 10_000_000;

    private final MonitorQueue monitoringQueue = new MonitorQueue();
    private MonitorLogWriter monitorLogWriter;
    private Thread monitorLogWriterThread;

    public IntersectionSimulationTopicDeleter() {
        super();
    }

    @Override
    public void run() {
        init();

        Random randomEngine = new Random();
        int nextStartIndex = startIndex;
        for (int i = 0; i < roundCount; i++) {
            nextStartIndex = f(1_000, 50_000, nextStartIndex, randomEngine);
            nextStartIndex = f(200, 10_000, nextStartIndex, randomEngine);
        }
        cleanupLogWriter();
    }

    private int f(int interval, int runningTime, int startIndex, Random randomEngine) {
        List<Integer> intervalNoises = NoiseUtils.generateNoiseList(
                interval * 0.3,
                interval / 2,
                NoiseUtils.SMALL_NOISE_LIST_LENGTH,
                randomEngine
        );

        Properties props = createAdminClientConfig();
        int totalCnt = startIndex;
        try (AdminClient adminClient = KafkaAdminClient.create(props)) {
            long endTime = TimeUtils.getAccurateCurrentTimeMillis() + runningTime;
            for (int i = 0; true; i++) {
                if (TimeUtils.getAccurateCurrentTimeMillis() >= endTime) break;
                long startTimestamp = TimeUtils.getAccurateCurrentTimeMillis();
                List<String> topicNames = new ArrayList<>();
                topicNames.add(topicPrefix + totalCnt);
                totalCnt += 1;
                doDeleteTopics(adminClient, topicNames);

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
        return totalCnt;
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

    private void doDeleteTopics(AdminClient adminClient, List<String> topicNames) {
        for (String topicName: topicNames) {
            doDeleteTopic(adminClient, topicName);
        }
    }

    private void doDeleteTopic(AdminClient adminClient, String topicName) {
        addMonitorLog(topicName, "REQUESTED");

        KafkaFuture<Void> future = adminClient.deleteTopics(List.of(topicName)).all();
    }

    private void addMonitorLog(String topicName, String status) {
        long timestamp = TimeUtils.getCurrentTimeMillis();
        long timestampNano = TimeUtils.getCurrentTimeNanos();
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

        new CommandLine(new IntersectionSimulationTopicDeleter())
                .execute(args);
    }
}
