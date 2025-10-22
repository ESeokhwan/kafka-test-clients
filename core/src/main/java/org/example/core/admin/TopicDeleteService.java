package org.example.core.admin;

import moniq.MonitorLog;
import moniq.MonitorQueue;
import moniq.writer.MonitorLogWriter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.KafkaFuture;
import org.example.core.AbstractService;
import org.example.core.util.TimeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class TopicDeleteService extends AbstractService {

    private final String topicPrefix;
    private final int startIndex;
    private final int roundCnt;
    private final int perRoundCnt;
    private final boolean isBatch;
    private final boolean isSync;
    private final boolean ignoreResponse;
    private final boolean logEnabled;

    private final MonitorQueue monitoringQueue;
    private final MonitorLogWriter monitorLogWriter;

    private final AdminClient adminClient;
    private final boolean needToCleanupClient;

    private final AtomicInteger curIdx = new AtomicInteger(0);

    public TopicDeleteService(
            String brokers, String clientId, String topicPrefix, int startIndex, int roundCnt, int perRoundCnt,
            int interval, double intervalNoiseStddev, int intervalMaxAbsNoise, Random randomEngine,
            boolean isBatch, boolean isSync, boolean ignoreResponse, boolean logEnabled, MonitorQueue monitoringQueue, MonitorLogWriter monitorLogWriter
    ) {
        super(roundCnt, interval, intervalNoiseStddev, intervalMaxAbsNoise, randomEngine);
        this.topicPrefix = topicPrefix;
        this.startIndex = startIndex;
        this.roundCnt = roundCnt;
        this.perRoundCnt = perRoundCnt;
        this.isBatch = isBatch;
        this.isSync = isSync;
        this.ignoreResponse = ignoreResponse;
        this.logEnabled = logEnabled;
        this.monitoringQueue = monitoringQueue;
        this.monitorLogWriter = monitorLogWriter;

        Properties clientProps = createAdminClientConfig(brokers, clientId);
        this.adminClient = AdminClient.create(clientProps);
        this.needToCleanupClient = true;
    }

    public TopicDeleteService(
            AdminClient adminClient, String topicPrefix, int startIndex, int roundCnt, int perRoundCnt,
            int interval, double intervalNoiseStddev, int intervalMaxAbsNoise, Random randomEngine,
            boolean isBatch, boolean isSync, boolean ignoreResponse, boolean logEnabled, MonitorQueue monitoringQueue, MonitorLogWriter monitorLogWriter
    ) {
        super(roundCnt, interval, intervalNoiseStddev, intervalMaxAbsNoise, randomEngine);
        this.topicPrefix = topicPrefix;
        this.startIndex = startIndex;
        this.roundCnt = roundCnt;
        this.perRoundCnt = perRoundCnt;
        this.isBatch = isBatch;
        this.isSync = isSync;
        this.ignoreResponse = ignoreResponse;
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
        return curIdx.get() >= roundCnt;
    }

    @Override
    public void work() {
        int curTopicIdx = startIndex + curIdx.getAndIncrement() * perRoundCnt;
        List<String> topics = new ArrayList<>();
        for (int i = 0; i < perRoundCnt; i++) topics.add(topicPrefix + "_" + (curTopicIdx + i));
        doDeleteTopics(topics);
    }

    @Override
    public void close() {
        if (adminClient != null && needToCleanupClient) adminClient.close();
    }

    private void doDeleteTopics(List<String> topicNames) {
        try {
            if (isBatch) {
                doDeleteTopicBatch(topicNames);
                return;
            }

            for (String topicName : topicNames) {
                doDeleteTopic(topicName);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void doDeleteTopic(String topicName) throws InterruptedException {
        appendMonitorLog(topicName, "REQUESTED");

        KafkaFuture<Void> future = adminClient.deleteTopics(List.of(topicName)).all();
        if (!ignoreResponse) future.whenComplete(handleResponse(List.of(topicName)));
        if (isSync) {
            try {
                future.get(); // Wait for the deletion to complete if not async
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void doDeleteTopicBatch(List<String> topicNames) throws InterruptedException {
        long stTimestamp = TimeUtils.getCurrentTimeMillis();
        long stTimestampNano = TimeUtils.getCurrentTimeNanos();

        KafkaFuture<Void> future = adminClient.deleteTopics(topicNames).all();
        if (!ignoreResponse) future.whenComplete(handleResponse(topicNames));

        for (String topicName: topicNames) appendMonitorLog(topicName, "REQUESTED", stTimestamp, stTimestampNano);
        if (isSync) {
            try {
                future.get(); // Wait for the deletion to complete if sync
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private KafkaFuture.BiConsumer<Void, Throwable> handleResponse(List<String> topicNames) {
        return (res, ex) -> {
            long enTimestamp = TimeUtils.getCurrentTimeMillis();
            long enTimestampNano = TimeUtils.getCurrentTimeNanos();
            if (ex != null) {
                for (String topicName : topicNames) appendMonitorLog(topicName, "FAILED", enTimestamp, enTimestampNano);
                return;
            }
            for (String topicName : topicNames) appendMonitorLog(topicName, "RESPONDED", enTimestamp, enTimestampNano);
        };
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
