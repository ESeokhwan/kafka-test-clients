package org.example.apps;

import moniq.MonitorLog;
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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class AutoTopicProducer extends AbstractCommand implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(AutoTopicProducer.class);

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

    @Option(names = {"--log-disabled"}, description = "If true, monitor log will be disabled. Default: false")
    private boolean logDisabled = false;

    @Option(names = {"--topic-create-log-file"}, description = "Optional output file for auto-topic creation timing logs")
    private String topicCreateLogFile;

    @Option(names = {"--produce-log-file"}, description = "Optional output file for produce timing logs")
    private String produceLogFile;

    @Option(names = {"--start-barrier-delay"}, description = "Delay (ms) before starting the production. Default: 5000")
    private int startBarrierDelay = 5000;

    @Option(names = "--monitoring-batch-size", description = "Batch size for monitoring log writing. Default: 10,000,000")
    private int monitoringBatchSize = 10_000_000;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger activeWorkers = new AtomicInteger(0);
    private final List<Thread> workerThreads = new ArrayList<>();
    private Producer<String, String> sharedProducer;
    private IMessageAdaptor messageAdaptor;
    private DirectMonitorLogWriter topicCreateLogWriter;
    private DirectMonitorLogWriter produceLogWriter;

    private final Thread emergencyCleanupHook = new Thread(() -> {
        log.info("Shutdown hook triggered, exiting application.");
        running.set(false);
        cleanupWorkers();
        cleanupMonitor();
    });

    public static void main(String[] args) {
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
        String pid = rt.getName();
        ThreadContext.put("PID", pid);

        new CommandLine(new AutoTopicProducer())
                .execute(args);
    }

    @Override
    public void run() {
        validateOptions();
        Runtime.getRuntime().addShutdownHook(emergencyCleanupHook);
        initLogging();
        messageAdaptor = new NaiveMessageGenerator(msgSize, Math.min(msgSize, 1000));
        if (shareProducer) {
            sharedProducer = new KafkaProducer<>(createProducerConfig(prefix + "_shared"));
        }

        log.info(
                "AutoTopicProducer starting. topicCreateHz={}, topicCreateIntervalMs={}, expectedActiveTopics={}, runDurationMinutes={}, topicCnt={}, topicLifetimeMs={}, produceHzPerTopic={}, expectedSteadyRate={} msg/sec",
                topicCreateHz,
                TimeUnit.NANOSECONDS.toMillis(hzToIntervalNanos(topicCreateHz)),
                expectedActiveTopics(),
                runDurationMinutes,
                effectiveTopicCnt(),
                effectiveTopicLifetimeMs(),
                produceHzPerTopic,
                expectedActiveTopics() * produceHzPerTopic
        );

        startBarrier(startBarrierDelay);
        launchTopicWorkers();
        joinWorkers();
        cleanupLogging();
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
                    "AutoTopicProducer-" + topicName
            );
            workerThreads.add(workerThread);
            workerThread.start();

            nextLaunchNanos += topicCreateIntervalNanos;
        }
    }

    private void joinWorkers() {
        for (Thread workerThread : workerThreads) {
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
        for (Thread workerThread : workerThreads) {
            workerThread.interrupt();
        }
        if (sharedProducer != null) {
            sharedProducer.flush();
            sharedProducer.close();
        }
    }

    private void initLogging() {
        if (logDisabled) {
            return;
        }

        if (topicCreateLogFile != null || produceLogFile != null) {
            try {
                if (topicCreateLogFile != null) {
                    topicCreateLogWriter = new DirectMonitorLogWriter(topicCreateLogFile);
                }
                if (produceLogFile != null) {
                    produceLogWriter = new DirectMonitorLogWriter(produceLogFile);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to initialize direct monitor log files", e);
            }
            return;
        }

        initMonitor(monitoringBatchSize);
    }

    private void cleanupLogging() {
        if (topicCreateLogWriter != null) {
            topicCreateLogWriter.close();
        }
        if (produceLogWriter != null) {
            produceLogWriter.close();
        }
        cleanupMonitor();
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

            long lifetimeNanos = TimeUnit.MILLISECONDS.toNanos(effectiveTopicLifetimeMs());
            long produceIntervalNanos = hzToIntervalNanos(produceHzPerTopic);
            long startedNanos = System.nanoTime();
            long deadlineNanos = startedNanos + lifetimeNanos;
            long nextProduceNanos = startedNanos;
            int sentCnt = 0;

            log.info("Topic worker started. topic={}, activeWorkers={}", topicName, activeWorkers.get());

            try {
                appendTopicCreateLog(topicName, "REQUESTED");
                while (running.get() && System.nanoTime() < deadlineNanos) {
                    sleepUntil(nextProduceNanos);
                    if (!running.get() || System.nanoTime() >= deadlineNanos) {
                        break;
                    }

                    String messageId = topicName + "_" + sentCnt;
                    String message = messageAdaptor.generate(messageId);
                    ProducerRecord<String, String> record = new ProducerRecord<>(topicName, message);

                    appendProduceLog(messageId, "REQUESTED");
                    producer.send(record, new TopicProducerCallback(topicName, messageId, sentCnt == 0));
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
                if (!shareProducer) {
                    producer.flush();
                    producer.close();
                }
                int remain = activeWorkers.decrementAndGet();
                log.info("Topic worker stopped. topic={}, sentCnt={}, activeWorkers={}", topicName, sentCnt, remain);
            }
        }
    }

    private class TopicProducerCallback implements Callback {
        private final String topicName;
        private final String messageId;
        private final boolean firstMessage;

        private TopicProducerCallback(String topicName, String messageId, boolean firstMessage) {
            this.topicName = topicName;
            this.messageId = messageId;
            this.firstMessage = firstMessage;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (ignoreResponse) {
                return;
            }
            String state = exception == null ? "RESPONDED" : "FAILED";
            if (firstMessage) {
                appendTopicCreateLog(topicName, state);
            }
            appendProduceLog(messageId, state);
        }
    }

    private void appendTopicCreateLog(String key, String status) {
        appendMonitorLog(topicCreateLogWriter, "TopicCreate", key, status);
    }

    private void appendProduceLog(String key, String status) {
        appendMonitorLog(produceLogWriter, "Producer", key, status);
    }

    private void appendMonitorLog(DirectMonitorLogWriter directWriter, String requestType, String key, String status) {
        if (logDisabled) {
            return;
        }

        long timestamp = TimeUtils.getCurrentTimeMillis();
        long timestampNano = TimeUtils.getCurrentTimeNanos();

        if (directWriter != null) {
            directWriter.write(requestType, key, timestamp, timestampNano, status);
            return;
        }

        if (isDirectLogMode()) {
            return;
        }

        monitoringQueue.enqueue(new MonitorLog(
                requestType, key, status, timestamp, timestampNano
        ));
        monitorLogWriter.notifyIfNeeded();
    }

    private boolean isDirectLogMode() {
        return topicCreateLogFile != null || produceLogFile != null;
    }

    private static class DirectMonitorLogWriter implements AutoCloseable {
        private final PrintWriter writer;

        private DirectMonitorLogWriter(String filePath) throws IOException {
            this.writer = new PrintWriter(new BufferedWriter(new FileWriter(filePath, false)));
            this.writer.println("RequestType, Id, Timestamp, TimestampNano, State");
        }

        private synchronized void write(String requestType, String id, long timestamp, long timestampNano, String state) {
            writer.printf("%s, %s, %d, %d, %s%n", requestType, id, timestamp, timestampNano, state);
        }

        @Override
        public synchronized void close() {
            writer.flush();
            writer.close();
        }
    }
}
