package org.example.apps;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import moniq.MonitorQueue;
import moniq.util.IMessageAdaptor;
import moniq.util.NaiveMessageGenerator;
import moniq.writer.MonitorLogWriter;
import moniq.writer.strategy.ScrapableWriteStrategy;
import org.apache.logging.log4j.ThreadContext;
import org.example.producer.ProducerRun;
import org.example.producer.Service;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class BasicProducer implements Runnable {

    @Getter
    @Option(names = {"-b", "--brokers"}, required = true, description = "Kafka Brokers (comma-separated list)")
    private String brokers;

    @Getter
    @Option(names = {"-p", "--prefix"}, required = true, description = "Prefix for topic, client, and etc.")
    private String prefix;

    @Getter
    @Option(names = {"--client-cnt"}, description = "Number of clients. Default: 1")
    private int clientCnt = 1;

    @Getter
    @Option(names = {"--topic-cnt-per-client"}, description = "Number of topics per each client. Default: 1")
    private int topicCntPerClient = 1;

    @Getter
    @Option(names = {"--msg-cnt-per-topic", "-n"}, description = "Number of messages of each client and topics. Default: 1")
    private int msgCntPerTopic = 1;

    @Getter
    @Option(names = {"--interval", "-i"}, description = "Produce interval (ms). Default: 1000")
    private int interval = 1000;

    @Getter
    @Option(names = {"--interval-noise-stddev"}, description = "Noise standard deviation of produce interval (ms). Default: 0")
    private double intervalNoiseStddev = 0;

    @Getter
    @Option(names = {"--interval-btw-topic"}, description = "The interval between produce requests for each topics (ms)." +
            " If this value is not -1, clients send produce requests serially, topic by topic. If it is -1," +
            " clients send produce requests to all topics concurrently. Default: -1")
    private int intervalBtwTopic = -1;

    @Getter
    @Option(names = {"--interval-noise-stddev-btw-topic"}, description = "Noise standard deviation of produce interval between topics (ms). Default: 0")
    private double intervalNoiseStddevBtwTopic = 0;

    @Getter
    @Option(names = {"--msg-size", "-m"}, description = "Message size in bytes. Default: 1000")
    private int msgSize = 1000;

    @Getter
    @Option(names = {"--is-sync"}, description = "If true, topic creation will be done synchronously. Default: false")
    private boolean isSync = false;

    @Getter
    @Option(names = {"--need-flush"}, description = "If true, flush will be called after producing messages. Default: false")
    private boolean needFlush = false;

    @Getter
    @Option(names = {"--sample-log"}, description = "If true, log sampling is enabled. Default: false")
    private boolean sampleLog = false;

    @Getter
    @Option(names = {"--tag-record"}, description = "If true, records will be tagged. Default: false")
    private boolean tagRecord = false;

    @Getter
    @Option(names = {"--scrapable"}, description = "If true, scrapable mode is enabled. Default: false")
    private boolean scrapable = false;

    @Getter
    @Option(names = {"--warmup-cnt"}, description = "Warm-up count before measurement. Default: 0")
    private int warmupCnt = 0;

    @Getter
    @Option(names = {"--warmup-topic"}, description = "Topic name for warm-up. Default: test_warmup")
    private String warmupTopic = "test_warmup";

    @Getter
    @Option(names = {"--start-barrier-delay"}, description = "Delay (ms) before starting the production. Default: 0")
    private int startBarrierDelay = 5000;

    @Getter
    @Option(names = "--monitoring-batch-size", description = "Batch size for monitoring log writing. Default: 10,000,000")
    private int monitoringBatchSize = 10_000_000;

    private final CountDownLatch startSignal = new CountDownLatch(1);

    private final List<ProducerRun> producersByClients = new ArrayList<>();
    private final List<Thread> producerThreads = new ArrayList<>();

    private IMessageAdaptor messageAdaptor;
    private MonitorQueue monitoringQueue;
    private MonitorLogWriter monitorLogWriter;
    private Thread monitorLogWriterThread;

    private final Thread emergencyCleanupHook = new Thread(() -> {
        log.info("Shutdown hook triggered, exiting application.");
        cleanup();
    });

    public BasicProducer() {
        super();
    }

    public static void main(String[] args) {
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
        String pid = rt.getName();
        ThreadContext.put("PID", pid);

        BasicProducer app = new BasicProducer();

        new CommandLine(app).execute(args);
    }

    @Override
    public void run() {
        init();

        for (ProducerRun producer: producersByClients) {
            Thread thread = new Thread(producer);
            producerThreads.add(thread);
            thread.start();
        }

        try {
            log.info("En Garde...");
            Thread.sleep(startBarrierDelay);
        } catch (InterruptedException e) {
            log.error("Thread interrupted during sleep", e);
            Thread.currentThread().interrupt();
        }
        log.info("Allez!");
        startSignal.countDown();
        waitAndCleanup();
    }

    private void init() {
        Runtime.getRuntime().addShutdownHook(emergencyCleanupHook);
        initMonitor();
        initProducers();
    }

    private void initMonitor() {
        messageAdaptor = new NaiveMessageGenerator(msgSize, Math.min(msgSize, 1000));
        monitoringQueue = new MonitorQueue();
        monitorLogWriter = new MonitorLogWriter(
                monitoringQueue,
                new ScrapableWriteStrategy(System.out),
                monitoringBatchSize
        );
        monitorLogWriterThread = new Thread(monitorLogWriter);
        monitorLogWriterThread.start();
    }

    private void initProducers() {
        for (int i = 0; i < clientCnt; i++) {
            List<Service> services = new ArrayList<>();
            for (int j = 0; j < topicCntPerClient; j++) {
                services.add(new Service(
                        brokers,
                        prefix + "_" + i,
                        prefix + "_" + i + "_" + j,
                        msgCntPerTopic,
                        interval,
                        intervalNoiseStddev,
                        interval / 2,
                        isSync,
                        needFlush,
                        (!sampleLog || i == 0),
                        tagRecord,
                        messageAdaptor,
                        monitoringQueue,
                        monitorLogWriter
                ));
            }
            Service warmupService = new Service(
                    brokers,
                    "warmup_" + i,
                    warmupTopic,
                    warmupCnt,
                    0,
                    0,
                    0,
                    false,
                    false,
                    false,
                    false,
                    messageAdaptor,
                    monitoringQueue,
                    monitorLogWriter
            );
            producersByClients.add(new ProducerRun(
                    services,
                    warmupService,
                    intervalBtwTopic,
                    intervalNoiseStddevBtwTopic,
                    intervalBtwTopic / 2, startSignal)
            );
        }
    }

    private void waitAndCleanup() {
        joinProducers();
        cleanupLogWriter();
        Runtime.getRuntime().removeShutdownHook(emergencyCleanupHook);
    }

    private void cleanup() {
        cleanupProducers();
        cleanupLogWriter();
    }

    private void cleanupProducers() {
        for (ProducerRun producer : producersByClients) producer.close();
        joinProducers();
    }

    private void joinProducers() {
        for (Thread thread: producerThreads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                log.error("Thread interrupted during join", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    private void cleanupLogWriter() {
        if (monitorLogWriter == null) return;

        monitorLogWriter.gracefulShutdown();
        monitorLogWriter.syncedNotify();
        try {
            monitorLogWriterThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
