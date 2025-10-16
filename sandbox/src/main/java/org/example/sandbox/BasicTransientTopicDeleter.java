package org.example.sandbox;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.Random;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.ThreadContext;
import org.example.core.AbstractCommand;
import org.example.core.IService;
import org.example.core.ServicesRunner;
import org.example.sandbox.services.TransientTopicDeleteService;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Slf4j
public class BasicTransientTopicDeleter extends AbstractCommand implements Runnable {

    @Getter
    @Option(names = {"-b", "--brokers"}, required = true, description = "Kafka Brokers (comma-separated list)")
    private String brokers;

    @Getter
    @Option(names = {"-p", "--prefix"}, required = true, description = "Prefix of topic names to be deleted")
    private String prefix;

    @Getter
    @Option(names = {"-s", "--start-index"}, description = "Start index of topic names. It will be used with topicPrefix. Default: 0")
    private int startIndex = 0;

    @Getter
    @Option(names = {"-n", "--round-cnt"}, description = "Number of rounds. Default: 10")
    private int roundCnt = 10;

    @Getter
    @Option(names = {"-i", "--interval"}, description = "Interval between each round in milli seconds. Default: 1,000")
    private int interval = 1000;

    @Getter
    @Option(names = "--interval-noise-stddev", description = "Noise standard deviation of interval between each round in milli seconds. Default: 0")
    private double noiseStddev = 0;

    @Getter
    @Option(names = "--per-round-cnt", description = "Number of topics in each round. Default: 1")
    private int perRoundCnt = 1;

    @Getter
    @Option(names = {"--is-batch"}, description = "If true, topic deletion will be batched for each round. Default: false")
    private boolean isBatch = false;

    @Getter
    @Option(names = {"--is-sync"}, description = "If true, topic deletion will be done synchronously. Default: false")
    private boolean isSync = false;

    @Getter
    @Option(names = {"--log-disabled"}, description = "If true, monitor log will be disabled. Default: false")
    private boolean logDisabled = false;

    @Getter
    @Option(names = {"--start-barrier-delay"}, description = "Delay (ms) before starting the production. Default: 0")
    private int startBarrierDelay = 5000;

    @Getter
    @Option(names = "--monitoring-batch-size", description = "Batch size for monitoring log writing. Default: 10,000,000")
    private int monitoringBatchSize = 10_000_000;

    private ServicesRunner serviceRunner;
    private Thread serviceRunnerThread;

    private final Thread emergencyCleanupHook = new Thread(() -> {
        log.info("Shutdown hook triggered, exiting application.");
        cleanupServicesRunner();
        cleanupMonitor();
    });

    public BasicTransientTopicDeleter() {
        super();
    }

    public static void main(String[] args) {
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
        String pid = rt.getName();
        ThreadContext.put("PID", pid);

        new CommandLine(new BasicTransientTopicDeleter())
                .execute(args);
    }

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(emergencyCleanupHook);
        initMonitor(monitoringBatchSize);
        initService();
        startBarrier(startBarrierDelay);

        joinServicesRunner();
        cleanupMonitor();
        Runtime.getRuntime().removeShutdownHook(emergencyCleanupHook);
    }

    private void initService() {
        Random randomEngine = new Random();
        List<IService> services = List.of(new TransientTopicDeleteService(
                brokers,
                prefix + "_deleter",
                prefix,
                startIndex,
                roundCnt,
                perRoundCnt,
                interval,
                noiseStddev,
                interval / 2,
                randomEngine,
                isBatch,
                isSync,
                !logDisabled,
                monitoringQueue,
                monitorLogWriter
        ));
        serviceRunner = new ServicesRunner(
                services, null, -1, 0,
                0, randomEngine, startSignal);

        serviceRunnerThread = new Thread(serviceRunner);
        serviceRunnerThread.start();
    }

    private void cleanupServicesRunner() {
        try {
            serviceRunner.close();
        } catch (IOException e) {
            log.error("Failed to close service runner", e);
            throw new RuntimeException(e);
        }
        joinServicesRunner();
    }

    private void joinServicesRunner() {
        try {
            serviceRunnerThread.join();
        } catch (InterruptedException e) {
            log.error("Thread interrupted during join", e);
            Thread.currentThread().interrupt();
        }
    }
}
