package org.example.producer;

import lombok.extern.slf4j.Slf4j;
import org.example.util.NoiseUtils;
import org.example.util.TimeUtils;

import java.io.Closeable;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ProducerRun implements Runnable, Closeable {

    private final List<Service> services;
    private final int interval;
    private final List<Integer> noises;
    private final CountDownLatch startSignal;
    private final CountDownLatch completionSignal;

    private int currentServiceIdx = 0;
    private final PriorityBlockingQueue<ScheduleEntry> scheduleQueue = new PriorityBlockingQueue<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public ProducerRun(List<Service> services, int interval, double intervalNoiseStddev, int intervalMaxAbsNoise, CountDownLatch startSignal) {
        this.services = services;
        this.interval = interval;
        this.startSignal = startSignal;
        this.completionSignal = new CountDownLatch(services.size());

        if (this.interval == -1) {
            this.noises = List.of();
        } else {
            Random randomEngine = new Random();
            this.noises = NoiseUtils.generateNoiseList(
                    intervalNoiseStddev, intervalMaxAbsNoise,
                    Math.min(services.size(), NoiseUtils.MAX_NOISE_LIST_LENGTH), randomEngine
            );
        }
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
        close();
    }

    @Override
    public void close() {
        log.info("Shutting down ProducerRun...");
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        for (Service service : services) service.close();
        while (completionSignal.getCount() > 0) completionSignal.countDown();
    }

    private void initFirstSchedules() {
        long curTime = TimeUtils.getAccurateCurrentTimeMillis();
        for (Service service : services) {
            long nextSchedule = curTime + service.curInterval();
            scheduleQueue.add(new ScheduleEntry(nextSchedule, service));
            if (interval != -1) break;
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

            if (scheduleEntry.service.hasMore()) {
                long nextScheduleTime = TimeUtils.getAccurateCurrentTimeMillis() + scheduleEntry.service.curInterval();
                scheduleQueue.add(new ScheduleEntry(nextScheduleTime, scheduleEntry.service));
            } else if (interval != -1 && currentServiceIdx < services.size() - 1) {
                long nextScheduleTime = TimeUtils.getAccurateCurrentTimeMillis() + interval + noises.get(currentServiceIdx % noises.size());
                currentServiceIdx += 1;
                scheduleQueue.add(new ScheduleEntry(nextScheduleTime, services.get(currentServiceIdx)));
            }

            ScheduleEntry nextEntry = scheduleQueue.poll();
            if (nextEntry != null) {
                long delay = Math.max(0, nextEntry.scheduledTime - TimeUtils.getAccurateCurrentTimeMillis());
                scheduler.schedule(new ProducerTask(nextEntry), delay, TimeUnit.MILLISECONDS);
            }

            if (!scheduleEntry.service.hasMore()) completionSignal.countDown();
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
