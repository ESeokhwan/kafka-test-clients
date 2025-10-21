package org.example.core.admin;

import moniq.MonitorLog;
import moniq.MonitorQueue;
import moniq.writer.MonitorLogWriter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.example.core.AbstractService;
import org.example.core.util.TimeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class TopicCreateService extends AbstractService {

    private final String topicPrefix;
    private final int partitionCnt;
    private final short replicationFactor;
    private final int startIndex;
    private final int roundCnt;
    private final int perRoundCnt;
    private final boolean isBatch;
    private final boolean isSync;
    private final boolean logEnabled;

    private final MonitorQueue monitoringQueue;
    private final MonitorLogWriter monitorLogWriter;

    private final AdminClient adminClient;
    private final boolean needToCleanupClient;

    private int curIdx = 0;

    public TopicCreateService(
            String brokers, String clientId, String topicPrefix, int partitionCnt, short replicationFactor, int startIndex, int roundCnt, int perRoundCnt,
            int interval, double intervalNoiseStddev, int intervalMaxAbsNoise, Random randomEngine,
            boolean isBatch, boolean isSync, boolean logEnabled, MonitorQueue monitoringQueue, MonitorLogWriter monitorLogWriter
    ) {
        super(roundCnt, interval, intervalNoiseStddev, intervalMaxAbsNoise, randomEngine);
        this.topicPrefix = topicPrefix;
        this.partitionCnt = partitionCnt;
        this.replicationFactor = replicationFactor;
        this.startIndex = startIndex;
        this.roundCnt = roundCnt;
        this.perRoundCnt = perRoundCnt;
        this.isBatch = isBatch;
        this.isSync = isSync;
        this.logEnabled = logEnabled;
        this.monitoringQueue = monitoringQueue;
        this.monitorLogWriter = monitorLogWriter;

        Properties clientProps = createAdminClientConfig(brokers, clientId);
        this.adminClient = AdminClient.create(clientProps);
        this.needToCleanupClient = true;
    }

    public TopicCreateService(
            AdminClient adminClient, String topicPrefix, int partitionCnt, short replicationFactor, int startIndex, int roundCnt, int perRoundCnt,
            int interval, double intervalNoiseStddev, int intervalMaxAbsNoise, Random randomEngine,
            boolean isBatch, boolean isSync, boolean logEnabled, MonitorQueue monitoringQueue, MonitorLogWriter monitorLogWriter
    ) {
        super(roundCnt, interval, intervalNoiseStddev, intervalMaxAbsNoise, randomEngine);
        this.topicPrefix = topicPrefix;
        this.partitionCnt = partitionCnt;
        this.replicationFactor = replicationFactor;
        this.startIndex = startIndex;
        this.roundCnt = roundCnt;
        this.perRoundCnt = perRoundCnt;
        this.isBatch = isBatch;
        this.isSync = isSync;
        this.logEnabled = logEnabled;
        this.monitoringQueue = monitoringQueue;
        this.monitorLogWriter = monitorLogWriter;

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
    public boolean isDone() {
        return curIdx >= roundCnt;
    }

    @Override
    public void work() {
        int curTopicIdx = startIndex + curIdx * perRoundCnt;
        List<String> topics = new ArrayList<>();
        for (int i = 0; i < perRoundCnt; i++) topics.add(topicPrefix + "_" + (curTopicIdx + i));
        doCreateTopics(topics);
        curIdx += 1;
    }

    @Override
    public void close() {
        if (adminClient != null && needToCleanupClient) adminClient.close();
    }

    private void doCreateTopics(List<String> topicNames) {
        if (isBatch) {
            doCreateTopicBatch(topicNames);
            return;
        }

        for (String topicName: topicNames) {
            doCreateTopic(topicName);
        }
    }

    private void doCreateTopic(String topicName) {
        appendMonitorLog(topicName, "REQUESTED");

        KafkaFuture<Void> future = adminClient.createTopics(
                List.of(new NewTopic(topicName, partitionCnt, replicationFactor))
        ).all();
        if (!isSync) return;

        try {
            future.get(); // Wait for the deletion to complete if not async
            appendMonitorLog(topicName, "RESPONDED");
        } catch (InterruptedException | ExecutionException e) {
            appendMonitorLog(topicName, "FAILED");
        }
    }

    private void doCreateTopicBatch(List<String> topicNames) {
        long stTimestamp = TimeUtils.getCurrentTimeMillis();
        long stTimestampNano = TimeUtils.getCurrentTimeNanos();

        List<NewTopic> topics = topicNames.stream().map(
                t -> new NewTopic(t, partitionCnt, replicationFactor)
        ).toList();
        KafkaFuture<Void> future = adminClient.createTopics(topics).all();
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
                "Creator", topic, status, timestamp, timestampNano
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
