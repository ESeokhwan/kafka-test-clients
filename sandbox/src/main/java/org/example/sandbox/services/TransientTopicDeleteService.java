package org.example.sandbox.services;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import moniq.MonitorLog;
import moniq.MonitorQueue;
import moniq.writer.MonitorLogWriter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.KafkaFuture;
import org.example.core.IService;
import org.example.core.util.NoiseUtils;
import org.example.core.util.Noises;
import org.example.core.util.TimeUtils;

public class TransientTopicDeleteService implements IService {

    private final String topicPrefix;
    private final int startIndex;
    private final int roundCnt;
    private final int perRoundCnt;
    private final int interval;
    private final Noises noises;
    private final boolean isBatch;
    private final boolean isSync;
    private final boolean logEnabled;

    private final MonitorQueue monitoringQueue;
    private final MonitorLogWriter monitorLogWriter;

    private final AdminClient adminClient;
    private final boolean needToCleanupClient;

    private int curIdx = 0;

    public TransientTopicDeleteService(
            String brokers, String clientId, String topicPrefix, int startIndex, int roundCnt, int perRoundCnt,
            int interval, double intervalNoiseStddev, int intervalMaxAbsNoise, Random randomEngine,
            boolean isBatch, boolean isSync, boolean logEnabled, MonitorQueue monitoringQueue, MonitorLogWriter monitorLogWriter
    ) {
        this.topicPrefix = topicPrefix;
        this.startIndex = startIndex;
        this.roundCnt = roundCnt;
        this.perRoundCnt = perRoundCnt;
        this.interval = interval;
        this.isBatch = isBatch;
        this.isSync = isSync;
        this.logEnabled = logEnabled;
        this.monitoringQueue = monitoringQueue;
        this.monitorLogWriter = monitorLogWriter;

        this.noises = NoiseUtils.generateNoises(
                intervalNoiseStddev, intervalMaxAbsNoise,
                Math.min(roundCnt, NoiseUtils.MAX_NOISE_LIST_LENGTH), randomEngine
        );
        Properties clientProps = createAdminClientConfig(brokers, clientId);
        this.adminClient = AdminClient.create(clientProps);
        this.needToCleanupClient = true;
    }

    public TransientTopicDeleteService(
            AdminClient adminClient, String topicPrefix, int startIndex, int roundCnt, int perRoundCnt,
            int interval, double intervalNoiseStddev, int intervalMaxAbsNoise, Random randomEngine,
            boolean isBatch, boolean isSync, boolean logEnabled, MonitorQueue monitoringQueue, MonitorLogWriter monitorLogWriter
    ) {
        this.topicPrefix = topicPrefix;
        this.startIndex = startIndex;
        this.roundCnt = roundCnt;
        this.perRoundCnt = perRoundCnt;
        this.interval = interval;
        this.isBatch = isBatch;
        this.isSync = isSync;
        this.logEnabled = logEnabled;
        this.monitoringQueue = monitoringQueue;
        this.monitorLogWriter = monitorLogWriter;

        this.noises = NoiseUtils.generateNoises(
                intervalNoiseStddev, intervalMaxAbsNoise,
                Math.min(roundCnt, NoiseUtils.MAX_NOISE_LIST_LENGTH), randomEngine
        );
        this.adminClient = adminClient;
        this.needToCleanupClient = false;
    }

    public static Properties createAdminClientConfig(String brokers, String clientId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("client.id", clientId);

        return props;
    }

    @Override
    public int curInterval() {
        int curNoise = noises.next();
        if (curIdx == 0) return Math.abs(curNoise);
        return interval + curNoise;
    }

    @Override
    public boolean hasMore() {
        return curIdx < roundCnt;
    }

    @Override
    public void work() {
        int curTopicIdx = startIndex + curIdx * perRoundCnt;
        List<String> topics = new ArrayList<>();
        for (int i = 0; i < perRoundCnt; i++) topics.add(topicPrefix + "_" + (curTopicIdx + i));
        doDeleteTopics(topics);
        curIdx += 1;
    }

    @Override
    public void close() {
        if (adminClient != null && needToCleanupClient) adminClient.close();
    }

    private void doDeleteTopics(List<String> topicNames) {
        if (isBatch) {
            doDeleteTopicBatch(topicNames);
            return;
        }

        for (String topicName: topicNames) {
            doDeleteTopic(topicName);
        }
    }

    private void doDeleteTopic(String topicName) {
        appendMonitorLog(topicName, "REQUESTED");

        KafkaFuture<Void> future = adminClient.deleteTransientTopics(List.of(topicName)).all();
        if (!isSync) return;

        try {
            future.get(); // Wait for the deletion to complete if not async
            appendMonitorLog(topicName, "RESPONDED");
        } catch (InterruptedException | ExecutionException e) {
            appendMonitorLog(topicName, "FAILED");
        }
    }

    private void doDeleteTopicBatch(List<String> topicNames) {
        long stTimestamp = TimeUtils.getCurrentTimeMillis();
        long stTimestampNano = TimeUtils.getCurrentTimeNanos();

        KafkaFuture<Void> future = adminClient.deleteTransientTopics(topicNames).all();
        if (!isSync) {
            for (String topicName: topicNames) {
                appendMonitorLog(topicName, "REQUESTED", stTimestamp, stTimestampNano);
            }
            return;
        }

        try {
            future.get(); // Wait for the deletion to complete if sync
            long enTimestamp = TimeUtils.getCurrentTimeMillis();
            long enTimestampNano = TimeUtils.getCurrentTimeNanos();
            for (String topicName: topicNames) {
                appendMonitorLog(topicName, "REQUESTED", stTimestamp, stTimestampNano);
                appendMonitorLog(topicName, "RESPONDED", enTimestamp, enTimestampNano);
            }
        } catch (InterruptedException | ExecutionException e) {
            long enTimestamp = TimeUtils.getCurrentTimeMillis();
            long enTimestampNano = TimeUtils.getCurrentTimeNanos();
            for (String topicName: topicNames) {
                appendMonitorLog(topicName, "REQUESTED", stTimestamp, stTimestampNano);
                appendMonitorLog(topicName, "FAILED", enTimestamp, enTimestampNano);
            }
        }
    }

    private void appendMonitorLog(String topic, String status, long timestamp, long timestampNano) {
        if (!logEnabled) return;
        monitoringQueue.enqueue(new MonitorLog(
                "Deleter", topic, status, timestamp, timestampNano
        ));
        monitorLogWriter.notifyIfNeeded();
    }

    private void appendMonitorLog(String topic, String status) {
        if (!logEnabled) return;
        long timestamp = TimeUtils.getCurrentTimeMillis();
        long timestampNano = TimeUtils.getCurrentTimeNanos();
        appendMonitorLog(topic, status, timestamp, timestampNano);
    }
}
