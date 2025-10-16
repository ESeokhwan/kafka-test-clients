package org.example.core;

import lombok.extern.slf4j.Slf4j;
import moniq.MonitorQueue;
import moniq.writer.MonitorLogWriter;
import moniq.writer.strategy.ScrapableWriteStrategy;

import java.util.concurrent.CountDownLatch;

@Slf4j
public class AbstractCommand {

    protected MonitorQueue monitoringQueue;
    protected MonitorLogWriter monitorLogWriter;
    protected Thread monitorLogWriterThread;

    protected final CountDownLatch startSignal = new CountDownLatch(1);

    protected void startBarrier(int delay) {
        try {
            log.info("En Garde...");
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            log.error("Thread interrupted during sleep", e);
            Thread.currentThread().interrupt();
        }
        log.info("Allez!");
        startSignal.countDown();
    }

    protected void initMonitor(int monitoringBatchSize) {
        monitoringQueue = new MonitorQueue();
        monitorLogWriter = new MonitorLogWriter(
                monitoringQueue,
                new ScrapableWriteStrategy(System.out),
                monitoringBatchSize
        );
        monitorLogWriterThread = new Thread(monitorLogWriter);
        monitorLogWriterThread.start();
    }

    protected void cleanupMonitor() {
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
