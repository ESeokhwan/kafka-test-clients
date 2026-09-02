package org.example.sandbox;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import moniq.util.IMessageAdaptor;
import moniq.util.NaiveMessageGenerator;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.ThreadContext;
import org.example.core.AbstractCommand;
import org.example.core.producer.ProducerService;
import org.example.core.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class CreateTopicAndProduceWorkload extends AbstractCommand implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(CreateTopicAndProduceWorkload.class);
    private static final double[] STATS_PERCENTILES = buildPercentiles();

    @Option(names = {"-b", "--brokers"}, required = true, description = "Kafka Brokers (comma-separated list)")
    private String brokers;

    @Option(names = {"-p", "--prefix"}, required = true, description = "Prefix for topic and client id")
    private String prefix;

    @Option(names = {"--start-index"}, description = "Start index of topic names. Default: 0")
    private int startIndex = 0;

    @Option(names = {"--client-index"}, description = "Client index segment in topic names. Default: 0")
    private int clientIndex = 0;

    @Option(names = {"--topic-cnt"}, description = "Total number of topics to start. If <= 0, derived from topic-create-hz and run-duration-minutes. Default: -1")
    private int topicCnt = -1;

    @Option(names = {"--topic-create-hz"}, description = "New topic producer start rate. Default: 5.0")
    private double topicCreateHz = 5.0;

    @Option(names = {"--run-duration-minutes"}, description = "Run duration used to derive total topic count. Default: 10")
    private double runDurationMinutes = 10.0;

    @Option(names = {"--topic-lifetime-ms"}, description = "Produce duration for each topic. Default: 60000")
    private long topicLifetimeMs = TimeUnit.MINUTES.toMillis(1);

    @Option(names = {"--produce-hz-per-topic"}, description = "Produce rate for each active topic. Default: 100.0")
    private double produceHzPerTopic = 100.0;

    @Option(names = {"--msg-size", "-m"}, description = "Message size in bytes. Default: 1000")
    private int msgSize = 1000;

    @Option(names = {"--share-producer"}, description = "If true, one KafkaProducer is shared by all topic workers. Default: false")
    private boolean shareProducer = false;

    @Option(names = {"--ignore-response"}, description = "If true, producer will ignore responses from Kafka brokers. Default: false")
    private boolean ignoreResponse = false;

    @Option(names = {"--need-flush"}, description = "If true, flush after each send. Default: false")
    private boolean needFlush = false;

    @Option(names = {"--log-disabled"}, description = "Deprecated in metrics mode. Raw monitor logs are not written. Stats CSV is still written. Default: false")
    private boolean logDisabled = false;

    @Option(names = {"--stdout-monitor-log"}, description = "Deprecated in metrics mode. Raw monitor logs are not written. Default: false")
    private boolean stdoutMonitorLog = false;

    @Option(names = {"--topic-create-log-file"}, description = "Deprecated in metrics mode. Raw creation logs are not written.")
    private String topicCreateLogFile;

    @Option(names = {"--produce-log-file"}, description = "Deprecated in metrics mode. Raw produce logs are not written.")
    private String produceLogFile;

    @Option(names = {"--topic-create-stats-file"}, description = "Deprecated. Topic creation latency is included in --metrics-file.")
    private String topicCreateStatsFile;

    @Option(names = {"--metrics-file", "--produce-stats-file"}, description = "Output CSV file for per-topic produce statistics plus topic creation latency")
    private String metricsFile = "create_topic_and_produce_workload_metrics.csv";

    @Option(names = {"--stats-disabled"}, description = "If true, per-topic stats CSV files will not be created. Default: false")
    private boolean statsDisabled = false;

    @Option(names = {"--start-barrier-delay"}, description = "Delay (ms) before starting the production. Default: 5000")
    private int startBarrierDelay = 5000;

    @Option(names = "--monitoring-batch-size", description = "Batch size for monitoring log writing. Default: 10,000,000")
    private int monitoringBatchSize = 10_000_000;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean metricsWritten = new AtomicBoolean(false);
    private final AtomicInteger activeWorkers = new AtomicInteger(0);
    private final List<Thread> workerThreads = Collections.synchronizedList(new ArrayList<>());
    private final List<TopicMetricsResult> topicResults = Collections.synchronizedList(new ArrayList<>());
    private Producer<String, String> sharedProducer;
    private IMessageAdaptor messageAdaptor;

    private final Thread emergencyCleanupHook = new Thread(() -> {
        log.info("Shutdown hook triggered, exiting application.");
        running.set(false);
        cleanupWorkers();
        waitForWorkers(TimeUnit.SECONDS.toMillis(10));
        writeMetricsFile();
        cleanupMonitor();
    });

    public static void main(String[] args) {
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
        ThreadContext.put("PID", rt.getName());

        new CommandLine(new CreateTopicAndProduceWorkload()).execute(args);
    }

    @Override
    public void run() {
        validateOptions();
        Runtime.getRuntime().addShutdownHook(emergencyCleanupHook);
        warnDeprecatedOutputOptions();
        messageAdaptor = new NaiveMessageGenerator(msgSize, Math.min(msgSize, 1000));
        if (shareProducer) {
            sharedProducer = new KafkaProducer<>(createProducerConfig(prefix + "_shared"));
        }

        log.info(
                "CreateTopicAndProduceWorkload starting. topicCreateHz={}, topicCreateIntervalMs={}, expectedActiveTopics={}, runDurationMinutes={}, topicCnt={}, topicLifetimeMs={}, produceHzPerTopic={}, expectedSteadyRate={} msg/sec, metricsFile={}, statsDisabled={}",
                topicCreateHz,
                TimeUnit.NANOSECONDS.toMillis(hzToIntervalNanos(topicCreateHz)),
                expectedActiveTopics(),
                runDurationMinutes,
                effectiveTopicCnt(),
                effectiveTopicLifetimeMs(),
                produceHzPerTopic,
                expectedActiveTopics() * produceHzPerTopic,
                normalizeCsvPath(metricsFile),
                statsDisabled
        );

        startBarrier(startBarrierDelay);
        launchTopicWorkers();
        joinWorkers();
        writeMetricsFile();
        Runtime.getRuntime().removeShutdownHook(emergencyCleanupHook);
    }

    private void validateOptions() {
        if (topicCreateHz <= 0.0) {
            throw new IllegalArgumentException("--topic-create-hz must be positive");
        }
        if (topicCreateHz > 20.0) {
            throw new IllegalArgumentException("--topic-create-hz must be <= 20");
        }
        if (runDurationMinutes <= 0.0) {
            throw new IllegalArgumentException("--run-duration-minutes must be positive");
        }
        if (produceHzPerTopic <= 0.0) {
            throw new IllegalArgumentException("--produce-hz-per-topic must be positive");
        }
        if (msgSize <= 0) {
            throw new IllegalArgumentException("--msg-size must be positive");
        }
        if (ignoreResponse && !statsDisabled) {
            log.warn("--ignore-response is enabled. Response latency metrics will be recorded as missing responses.");
        }
    }

    private void warnDeprecatedOutputOptions() {
        if (logDisabled || stdoutMonitorLog || topicCreateLogFile != null || produceLogFile != null) {
            log.warn("Raw monitor log options are ignored by CreateTopicAndProduceWorkload. Only end-of-run CSV metrics are written.");
        }
        if (topicCreateStatsFile != null) {
            log.warn("--topic-create-stats-file is ignored. Topic creation latency is included as TopicCreateLatencyMs in {}.", normalizeCsvPath(metricsFile));
        }
    }

    private void launchTopicWorkers() {
        long topicCreateIntervalNanos = hzToIntervalNanos(topicCreateHz);
        long nextLaunchNanos = System.nanoTime();
        int targetTopicCnt = effectiveTopicCnt();

        for (int i = 0; i < targetTopicCnt && running.get(); i++) {
            sleepUntil(nextLaunchNanos);

            int topicIndex = startIndex + i;
            String topicName = prefix + "_" + clientIndex + "_" + topicIndex;
            Thread workerThread = new Thread(
                    new TopicWorker(topicName, prefix + "_" + clientIndex + "_" + topicIndex),
                    "CreateTopicAndProduceWorkload-" + topicName
            );
            workerThreads.add(workerThread);
            workerThread.start();

            nextLaunchNanos += topicCreateIntervalNanos;
        }
    }

    private void joinWorkers() {
        for (Thread workerThread : workerThreadSnapshot()) {
            try {
                workerThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running.set(false);
                break;
            }
        }

        cleanupWorkers();
    }

    private void cleanupWorkers() {
        for (Thread workerThread : workerThreadSnapshot()) {
            workerThread.interrupt();
        }
        if (sharedProducer != null) {
            sharedProducer.flush();
            sharedProducer.close();
        }
    }

    private void waitForWorkers(long timeoutMs) {
        long deadlineMs = System.currentTimeMillis() + Math.max(0L, timeoutMs);
        for (Thread workerThread : workerThreadSnapshot()) {
            long remainingMs = deadlineMs - System.currentTimeMillis();
            if (remainingMs <= 0L) {
                return;
            }
            try {
                workerThread.join(remainingMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private List<Thread> workerThreadSnapshot() {
        synchronized (workerThreads) {
            return new ArrayList<>(workerThreads);
        }
    }

    private long effectiveTopicLifetimeMs() {
        return topicLifetimeMs;
    }

    private int effectiveTopicCnt() {
        if (topicCnt > 0) {
            return topicCnt;
        }
        return Math.max(1, (int) Math.ceil(topicCreateHz * 60.0 * runDurationMinutes));
    }

    private int expectedActiveTopics() {
        return Math.max(1, (int) Math.ceil(topicCreateHz * effectiveTopicLifetimeMs() / 1000.0));
    }

    private long hzToIntervalNanos(double hz) {
        return Math.max(1L, Math.round(1_000_000_000.0 / hz));
    }

    private Properties createProducerConfig(String clientId) {
        return ProducerService.createProducerConfig(brokers, clientId, !ignoreResponse);
    }

    private void sleepUntil(long targetNanos) {
        while (running.get()) {
            long remainingNanos = targetNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                return;
            }

            try {
                TimeUnit.NANOSECONDS.sleep(remainingNanos);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running.set(false);
                return;
            }
        }
    }

    private class TopicWorker implements Runnable {
        private final String topicName;
        private final String clientId;

        private TopicWorker(String topicName, String clientId) {
            this.topicName = topicName;
            this.clientId = clientId;
        }

        @Override
        public void run() {
            activeWorkers.incrementAndGet();
            Producer<String, String> producer = shareProducer
                    ? sharedProducer
                    : new KafkaProducer<>(createProducerConfig(clientId));

            TopicMetrics metrics = new TopicMetrics(topicName);
            long lifetimeNanos = TimeUnit.MILLISECONDS.toNanos(effectiveTopicLifetimeMs());
            long produceIntervalNanos = hzToIntervalNanos(produceHzPerTopic);
            long startedNanos = System.nanoTime();
            long deadlineNanos = startedNanos + lifetimeNanos;
            long nextProduceNanos = startedNanos;
            int sentCnt = 0;

            log.info("Topic worker started. topic={}, activeWorkers={}", topicName, activeWorkers.get());

            long topicCreateRequestMs = TimeUtils.getCurrentTimeMillis();
            try {
                while (running.get() && System.nanoTime() < deadlineNanos) {
                    sleepUntil(nextProduceNanos);
                    if (!running.get() || System.nanoTime() >= deadlineNanos) {
                        break;
                    }

                    String messageId = topicName + "_" + sentCnt;
                    String message = messageAdaptor.generate(messageId);
                    ProducerRecord<String, String> record = new ProducerRecord<>(topicName, message);
                    long requestMs = TimeUtils.getCurrentTimeMillis();

                    producer.send(
                            record,
                            new TopicProducerCallback(
                                    sentCnt == 0,
                                    requestMs,
                                    topicCreateRequestMs,
                                    metrics
                            )
                    );
                    if (needFlush) {
                        producer.flush();
                    }

                    sentCnt++;
                    nextProduceNanos += produceIntervalNanos;
                    if (System.nanoTime() - nextProduceNanos > produceIntervalNanos) {
                        nextProduceNanos = System.nanoTime() + produceIntervalNanos;
                    }
                }
            } finally {
                producer.flush();
                if (!shareProducer) {
                    producer.close();
                }
                metrics.finish(sentCnt);
                if (!statsDisabled) {
                    topicResults.add(metrics.snapshotResult());
                }
                int remain = activeWorkers.decrementAndGet();
                log.info(
                        "Topic worker stopped. topic={}, sentCnt={}, responseCnt={}, missingCnt={}, activeWorkers={}",
                        topicName,
                        sentCnt,
                        metrics.produceSuccessCount(),
                        metrics.produceMissingCount(),
                        remain
                );
            }
        }
    }

    private class TopicProducerCallback implements Callback {
        private final boolean firstMessage;
        private final long requestMs;
        private final long topicCreateRequestMs;
        private final TopicMetrics metrics;

        private TopicProducerCallback(
                boolean firstMessage,
                long requestMs,
                long topicCreateRequestMs,
                TopicMetrics metrics
        ) {
            this.firstMessage = firstMessage;
            this.requestMs = requestMs;
            this.topicCreateRequestMs = topicCreateRequestMs;
            this.metrics = metrics;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (ignoreResponse) {
                return;
            }

            long responseMs = TimeUtils.getCurrentTimeMillis();
            if (exception == null) {
                metrics.addProduceLatency(responseMs - requestMs);
            }
            if (firstMessage) {
                if (exception == null) {
                    metrics.addTopicCreateLatency(responseMs - topicCreateRequestMs);
                }
            }
        }
    }

    private void writeMetricsFile() {
        if (statsDisabled) {
            return;
        }
        if (!metricsWritten.compareAndSet(false, true)) {
            return;
        }

        String filePath = normalizeCsvPath(metricsFile);
        List<TopicMetricsResult> rows;
        synchronized (topicResults) {
            rows = new ArrayList<>(topicResults);
        }
        rows.sort((left, right) -> naturalCompare(left.topicName, right.topicName));

        LatencyHistogram all = new LatencyHistogram();
        double topicCreateTotal = 0.0;
        long topicCreateCount = 0L;
        for (TopicMetricsResult row : rows) {
            all.merge(row.produceSnapshot);
            if (row.topicCreateLatencyMs != null) {
                topicCreateTotal += row.topicCreateLatencyMs;
                topicCreateCount++;
            }
        }
        String allTopicCreateLatency = topicCreateCount == 0L
                ? ""
                : metricValue(topicCreateTotal / topicCreateCount);

        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filePath, false)))) {
            writer.println(metricsHeader());
            writer.println(all.snapshot("ALL").toCsvRow(allTopicCreateLatency));
            for (TopicMetricsResult row : rows) {
                writer.println(row.produceSnapshot.toCsvRow(row.topicCreateLatencyValue()));
            }
            log.info("Wrote metrics CSV file {} rows={}", filePath, rows.size() + 1);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write metrics CSV file: " + filePath, e);
        }
    }

    private static String metricsHeader() {
        StringBuilder builder = new StringBuilder();
        builder.append("TestCreator,Sample Count,Missing Response Count,Mean(ms),Median(ms),Min(ms),Max(ms)");
        for (double percentile : STATS_PERCENTILES) {
            builder.append(",P").append(formatPercentileName(percentile)).append("(ms)");
        }
        builder.append(",TopicCreateLatencyMs");
        return builder.toString();
    }

    private static String normalizeCsvPath(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            return "create_topic_and_produce_workload_metrics.csv";
        }
        String trimmed = filePath.trim();
        return trimmed.toLowerCase(Locale.ROOT).endsWith(".csv") ? trimmed : trimmed + ".csv";
    }

    private static double[] buildPercentiles() {
        double[] values = new double[103];
        for (int i = 0; i <= 100; i++) {
            values[i] = i;
        }
        values[101] = 99.9;
        values[102] = 99.99;
        return values;
    }

    private static String formatPercentileName(double value) {
        if (value == Math.rint(value)) {
            return String.format(Locale.US, "%.0f", value);
        }
        return String.format(Locale.US, "%s", value);
    }

    private static String metricValue(double value) {
        double rounded = Math.round(value * 1_000_000.0) / 1_000_000.0;
        if (rounded == Math.rint(rounded)) {
            return String.format(Locale.US, "%.0f", rounded);
        }
        return String.format(Locale.US, "%.6f", rounded);
    }

    private static int naturalCompare(String left, String right) {
        List<Object> leftParts = naturalParts(left);
        List<Object> rightParts = naturalParts(right);
        int limit = Math.min(leftParts.size(), rightParts.size());
        for (int i = 0; i < limit; i++) {
            Object leftPart = leftParts.get(i);
            Object rightPart = rightParts.get(i);
            int result;
            if (leftPart instanceof Long && rightPart instanceof Long) {
                result = Long.compare((Long) leftPart, (Long) rightPart);
            } else {
                result = leftPart.toString().compareTo(rightPart.toString());
            }
            if (result != 0) {
                return result;
            }
        }
        return Integer.compare(leftParts.size(), rightParts.size());
    }

    private static List<Object> naturalParts(String value) {
        List<Object> parts = new ArrayList<>();
        StringBuilder text = new StringBuilder();
        StringBuilder digits = new StringBuilder();

        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (Character.isDigit(ch)) {
                if (text.length() > 0) {
                    parts.add(text.toString());
                    text.setLength(0);
                }
                digits.append(ch);
            } else {
                if (digits.length() > 0) {
                    parts.add(Long.parseLong(digits.toString()));
                    digits.setLength(0);
                }
                text.append(ch);
            }
        }

        if (text.length() > 0) {
            parts.add(text.toString());
        }
        if (digits.length() > 0) {
            parts.add(Long.parseLong(digits.toString()));
        }
        return parts;
    }

    private static String csvEscape(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        boolean needsEscape = value.indexOf(',') >= 0 || value.indexOf('"') >= 0 || value.indexOf('\n') >= 0;
        if (!needsEscape) {
            return value;
        }
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static class TopicMetrics {
        private final String topicName;
        private final LatencyHistogram produce = new LatencyHistogram();
        private Long topicCreateLatencyMs;

        private TopicMetrics(String topicName) {
            this.topicName = topicName;
        }

        private void addProduceLatency(long latencyMs) {
            produce.add(latencyMs);
        }

        private synchronized void addTopicCreateLatency(long latencyMs) {
            if (topicCreateLatencyMs == null) {
                topicCreateLatencyMs = latencyMs;
            }
        }

        private void finish(int sentCount) {
            long produceMissing = Math.max(0L, sentCount - produce.count());
            produce.addMissing(produceMissing);
        }

        private long produceSuccessCount() {
            return produce.count();
        }

        private long produceMissingCount() {
            return produce.missingCount();
        }

        private synchronized TopicMetricsResult snapshotResult() {
            return new TopicMetricsResult(topicName, produce.snapshot(topicName), topicCreateLatencyMs);
        }
    }

    private static class TopicMetricsResult {
        private final String topicName;
        private final StatsSnapshot produceSnapshot;
        private final Long topicCreateLatencyMs;

        private TopicMetricsResult(String topicName, StatsSnapshot produceSnapshot, Long topicCreateLatencyMs) {
            this.topicName = topicName;
            this.produceSnapshot = produceSnapshot;
            this.topicCreateLatencyMs = topicCreateLatencyMs;
        }

        private String topicCreateLatencyValue() {
            return topicCreateLatencyMs == null ? "" : metricValue(topicCreateLatencyMs);
        }
    }

    private static class LatencyHistogram {
        private final TreeMap<Long, Long> histogram = new TreeMap<>();
        private long count = 0L;
        private long missingCount = 0L;
        private double total = 0.0;
        private Long minValue;
        private Long maxValue;

        private synchronized void add(long value) {
            histogram.merge(value, 1L, Long::sum);
            count++;
            total += value;
            minValue = minValue == null ? value : Math.min(minValue, value);
            maxValue = maxValue == null ? value : Math.max(maxValue, value);
        }

        private synchronized void addMissing(long value) {
            missingCount += Math.max(0L, value);
        }

        private synchronized long count() {
            return count;
        }

        private synchronized long missingCount() {
            return missingCount;
        }

        private synchronized void merge(StatsSnapshot snapshot) {
            for (Map.Entry<Long, Long> entry : snapshot.histogram.entrySet()) {
                histogram.merge(entry.getKey(), entry.getValue(), Long::sum);
                count += entry.getValue();
                total += entry.getKey() * (double) entry.getValue();
                minValue = minValue == null ? entry.getKey() : Math.min(minValue, entry.getKey());
                maxValue = maxValue == null ? entry.getKey() : Math.max(maxValue, entry.getKey());
            }
            missingCount += snapshot.missingCount;
        }

        private synchronized StatsSnapshot snapshot(String groupName) {
            return new StatsSnapshot(
                    groupName,
                    new TreeMap<>(histogram),
                    count,
                    missingCount,
                    total,
                    minValue,
                    maxValue
            );
        }
    }

    private static class StatsSnapshot {
        private final String groupName;
        private final TreeMap<Long, Long> histogram;
        private final long count;
        private final long missingCount;
        private final double total;
        private final Long minValue;
        private final Long maxValue;

        private StatsSnapshot(
                String groupName,
                TreeMap<Long, Long> histogram,
                long count,
                long missingCount,
                double total,
                Long minValue,
                Long maxValue
        ) {
            this.groupName = groupName;
            this.histogram = histogram;
            this.count = count;
            this.missingCount = missingCount;
            this.total = total;
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        private String toCsvRow(String topicCreateLatencyValue) {
            StringBuilder builder = new StringBuilder();
            builder.append(csvEscape(groupName)).append(',');
            builder.append(count).append(',');
            builder.append(missingCount).append(',');

            if (count == 0) {
                builder.append(",,,,");
                for (int i = 0; i < STATS_PERCENTILES.length; i++) {
                    builder.append(',');
                }
                builder.append(',').append(topicCreateLatencyValue == null ? "" : topicCreateLatencyValue);
                return builder.toString();
            }

            builder.append(metricValue(total / count)).append(',');
            builder.append(metricValue(percentile(50.0))).append(',');
            builder.append(metricValue(minValue == null ? 0.0 : minValue)).append(',');
            builder.append(metricValue(maxValue == null ? 0.0 : maxValue));
            for (double percentile : STATS_PERCENTILES) {
                builder.append(',').append(metricValue(percentile(percentile)));
            }
            builder.append(',').append(topicCreateLatencyValue == null ? "" : topicCreateLatencyValue);
            return builder.toString();
        }

        private double percentile(double percentileValue) {
            if (count == 0) {
                return 0.0;
            }
            if (count == 1) {
                return valueAtIndex(0);
            }

            double rank = (percentileValue / 100.0) * (count - 1);
            long lowerIndex = (long) Math.floor(rank);
            long upperIndex = (long) Math.ceil(rank);
            double lowerValue = valueAtIndex(lowerIndex);
            if (lowerIndex == upperIndex) {
                return lowerValue;
            }
            double upperValue = valueAtIndex(upperIndex);
            return lowerValue + (upperValue - lowerValue) * (rank - lowerIndex);
        }

        private long valueAtIndex(long targetIndex) {
            long seen = 0L;
            for (Map.Entry<Long, Long> entry : histogram.entrySet()) {
                seen += entry.getValue();
                if (targetIndex < seen) {
                    return entry.getKey();
                }
            }
            return maxValue == null ? 0L : maxValue;
        }
    }
}
