package org.example.producer;

import lombok.extern.slf4j.Slf4j;
import org.example.util.TimeUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ProducerRun implements Runnable, Closeable {

    private final List<Service> services;
    private final CountDownLatch startSignal;

    private final PriorityBlockingQueue<ScheduleEntry> scheduleQueue = new PriorityBlockingQueue<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final CountDownLatch completionSignal = new CountDownLatch(1);

    public ProducerRun(List<Service> services, CountDownLatch startSignal) {
        this.services = services;
        this.startSignal = startSignal;
    }

    @Override
    public void run() {
        try {
            startSignal.await();
        } catch (InterruptedException e) {
            log.error("ProducerRun interrupted while waiting to start.", e);
            Thread.currentThread().interrupt();
        }
        if (completionSignal.getCount() == 0) return;

        initFirstSchedules();
        start();

        try {
            completionSignal.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        log.info("Shutting down ProducerRun...");
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        for (Service service : services) service.close();
        completionSignal.countDown();
    }

    private void initFirstSchedules() {
        long curTime = TimeUtils.getAccurateCurrentTimeMillis();
        for (Service service : services) {
            long nextSchedule = curTime + service.curInterval();
            scheduleQueue.add(new ScheduleEntry(nextSchedule, service));
        }
    }

    private void start() {
        ScheduleEntry initialEntry = scheduleQueue.poll();
        if (initialEntry != null) {
            long delay = Math.max(0, initialEntry.scheduledTime - TimeUtils.getAccurateCurrentTimeMillis());
            scheduler.schedule(new ProducerTask(initialEntry), delay, TimeUnit.MILLISECONDS);
        }
    }

    private class ProducerTask implements Runnable {
        private final ScheduleEntry scheduleEntry;

        public ProducerTask(ScheduleEntry scheduleEntry) {
            this.scheduleEntry = scheduleEntry;
        }

        @Override
        public void run() {
            if (completionSignal.getCount() == 0) return;
            scheduleEntry.service.produce();

            if (!scheduleEntry.service.hasMore()) {
                completionSignal.countDown();
                return;
            }
            long nextScheduleTime = TimeUtils.getAccurateCurrentTimeMillis() + scheduleEntry.service.curInterval();
            scheduleQueue.add(new ScheduleEntry(nextScheduleTime, scheduleEntry.service));

            ScheduleEntry nextEntry = scheduleQueue.poll();
            if (nextEntry != null) {
                long delay = Math.max(0, nextEntry.scheduledTime - TimeUtils.getAccurateCurrentTimeMillis());
                scheduler.schedule(new ProducerTask(nextEntry), delay, TimeUnit.MILLISECONDS);
            }
        }
    }

    private static class ScheduleEntry implements Comparable<ScheduleEntry> {

        private final long scheduledTime;
        private final Service service;

        public ScheduleEntry(long scheduledTime, Service service) {
            this.scheduledTime = scheduledTime;
            this.service = service;
        }

        @Override
        public int compareTo(ScheduleEntry other) {
            return Long.compare(this.scheduledTime, other.scheduledTime);
        }
    }
}
