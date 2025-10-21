package org.example.apps;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import moniq.util.IMessageAdaptor;
import moniq.util.NaiveMessageGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.logging.log4j.ThreadContext;
import org.example.core.AbstractCommand;
import org.example.core.IService;
import org.example.core.ServicesRunner;
import org.example.core.producer.ProducerService;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

@Slf4j
public class BasicProducer extends AbstractCommand implements Runnable {

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
    @Option(names = {"--share-producer"}, description = "If true, KafkaProducer instances will be shared among topics in each client. Default: false")
    private boolean shareProducer = false;

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

    @Getter
    @Option(names = "--init-scheduler-pool-size", description = "Initial scheduler pool size for services runner. Default: 8")
    private int initSchedulerPoolSize = 8;

    private final List<ServicesRunner> producersByClients = new ArrayList<>();
    private final List<Thread> producerThreads = new ArrayList<>();
    private final List<Producer<String, String>> sharedProducers = new ArrayList<>();

    private IMessageAdaptor messageAdaptor;

    private final Thread emergencyCleanupHook = new Thread(() -> {
        log.info("Shutdown hook triggered, exiting application.");
        cleanupProducers();
        cleanupMonitor();
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
        Runtime.getRuntime().addShutdownHook(emergencyCleanupHook);
        initMonitor(monitoringBatchSize);
        messageAdaptor = new NaiveMessageGenerator(msgSize, Math.min(msgSize, 1000));
        initServices();
        startBarrier(startBarrierDelay);

        joinProducers();
        for (Producer<String, String> producer: sharedProducers) producer.close();
        cleanupMonitor();
        Runtime.getRuntime().removeShutdownHook(emergencyCleanupHook);
    }

    private void initServices() {
        if (shareProducer) initSharingProdServices();
        else initStandaloneServices();

        for (ServicesRunner producer: producersByClients) {
            Thread thread = new Thread(producer);
            producerThreads.add(thread);
            thread.start();
        }
    }

    private void initStandaloneServices() {
        Random randomEngine = new Random();
        for (int i = 0; i < clientCnt; i++) {
            List<IService> services = new ArrayList<>();
            for (int j = 0; j < topicCntPerClient; j++) {
                services.add(new ProducerService(
                        brokers,
                        prefix + "_" + i,
                        prefix + "_" + i + "_" + j,
                        msgCntPerTopic,
                        interval,
                        intervalNoiseStddev,
                        interval / 2,
                        randomEngine,
                        isSync,
                        needFlush,
                        (!sampleLog || i == 0),
                        tagRecord,
                        messageAdaptor,
                        monitoringQueue,
                        monitorLogWriter
                ));
            }
            IService warmupService = new ProducerService(
                    brokers,
                    "warmup_" + i,
                    warmupTopic,
                    warmupCnt,
                    0,
                    0,
                    0,
                    randomEngine,
                    false,
                    true,
                    false,
                    false,
                    messageAdaptor,
                    monitoringQueue,
                    monitorLogWriter
            );
            producersByClients.add(new ServicesRunner(
                    services,
                    warmupService,
                    intervalBtwTopic,
                    intervalNoiseStddevBtwTopic,
                    intervalBtwTopic / 2,
                    randomEngine,
                    startSignal,
                    initSchedulerPoolSize
            ));
        }
    }

    private void initSharingProdServices() {
        Random randomEngine = new Random();
        for (int i = 0; i < clientCnt; i++) {
            List<IService> services = new ArrayList<>();
            Properties properties = ProducerService.createProducerConfig(brokers, prefix + "_" + i, isSync);
            Producer<String, String> producer = new KafkaProducer<>(properties);
            sharedProducers.add(producer);
            for (int j = 0; j < topicCntPerClient; j++) {
                services.add(new ProducerService(
                        producer,
                        prefix + "_" + i + "_" + j,
                        msgCntPerTopic,
                        interval,
                        intervalNoiseStddev,
                        interval / 2,
                        randomEngine,
                        isSync,
                        needFlush,
                        (!sampleLog || i == 0),
                        tagRecord,
                        messageAdaptor,
                        monitoringQueue,
                        monitorLogWriter
                ));
            }
            IService warmupService = new ProducerService(
                    producer,
                    warmupTopic,
                    warmupCnt,
                    0,
                    0,
                    0,
                    randomEngine,
                    false,
                    true,
                    false,
                    false,
                    messageAdaptor,
                    monitoringQueue,
                    monitorLogWriter
            );
            producersByClients.add(new ServicesRunner(
                    services,
                    warmupService,
                    intervalBtwTopic,
                    intervalNoiseStddevBtwTopic,
                    intervalBtwTopic / 2,
                    randomEngine,
                    startSignal,
                    initSchedulerPoolSize
            ));
        }
    }

    private void cleanupProducers() {
        for (ServicesRunner producer : producersByClients) {
            try {
                producer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        joinProducers();
        for (Producer<String, String> producer: sharedProducers) producer.close();
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
}
