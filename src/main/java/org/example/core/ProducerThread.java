package org.example.core;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import moniq.MonitorLog;
import moniq.MonitorQueue;
import moniq.util.IMessageAdaptor;
import moniq.writer.MonitorLogWriter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerThread implements Runnable {

    private static final long schedulingBatchSize = 10000L;

    private final Properties props;
    private final int index;
    private final List<ServiceInfo> services;

    private final Callback callback;
    private final AtomicBoolean startFlag;
    private final AtomicBoolean endFlag;

    private final IMessageAdaptor messageGenerator;
    private final MonitorLogWriter monitorLogWriter;
    private final MonitorQueue monitoringQueue;

    public ProducerThread(Properties props, int index, List<ServiceInfo> services,
                          Callback callback, AtomicBoolean startFlag, AtomicBoolean endFlag,
                          IMessageAdaptor messageGenerator, MonitorLogWriter monitorLogWriter,
                          MonitorQueue monitoringQueue) {
        this.props = props;
        this.index = index;
        this.services = services;
        this.callback = callback;
        this.startFlag = startFlag;
        this.endFlag = endFlag;
        this.messageGenerator = messageGenerator;
        this.monitorLogWriter = monitorLogWriter;
        this.monitoringQueue = monitoringQueue;
    }


    @Override
    public void run() {
        waitStartFlag();

        Properties usingProps = new Properties(this.props);
        usingProps.put("client.id", "producer-thread-" + index);
        try (Producer<String, String> producer = new KafkaProducer<>(usingProps)) {
            List<Integer> serviceCounts = generateServiceCounts();
            List<IndexWrapper<Schedule>> lastSchedules = generateLastSchedules();
            Queue<IndexWrapper<Schedule>> schedules = new LinkedList<>();
            while (!endFlag.get()) {
                if (schedules.isEmpty()) {
                    long curTime = System.currentTimeMillis();
                    schedules = getServiceSchedules(lastSchedules, curTime,curTime + schedulingBatchSize);
                }

                IndexWrapper<Schedule> curSchedule = schedules.poll();
                waitSchedule(curSchedule);

                int curServiceIndex = curSchedule.index();
                doProduce(producer, curSchedule, serviceCounts.get(curServiceIndex));
                serviceCounts.set(curServiceIndex, serviceCounts.get(curServiceIndex) + 1);
                lastSchedules.set(curServiceIndex, curSchedule);
            }
        } catch (Exception e) {
            e.printStackTrace();
            // Handle exception, possibly log it or rethrow it
        }
    }

    private void waitStartFlag() {
        while (!startFlag.get()) {
            Thread.yield();
        }
    }

    private List<Integer> generateServiceCounts() {
        List<Integer> serviceCounts = new ArrayList<>();
        for (int i = 0; i < services.size(); i++) {
            serviceCounts.add(0);
        }
        return serviceCounts;
    }

    private List<IndexWrapper<Schedule>> generateLastSchedules() {
        List<IndexWrapper<Schedule>> lastSchedules = new ArrayList<>();
        for (int i = 0; i < services.size(); i++) {
            lastSchedules.add(new IndexWrapper<>(new Schedule(services.get(i), 0), i));
        }
        return lastSchedules;
    }

    private Queue<IndexWrapper<Schedule>> getServiceSchedules(
        List<IndexWrapper<Schedule>> lastSchedules,
        long startTime,
        long endTime
    ) {
        List<Queue<IndexWrapper<Schedule>>> schedulesByServices = new ArrayList<>();
        for (IndexWrapper<Schedule> lastSchedule: lastSchedules) {
            ServiceInfo service = lastSchedule.value().serviceInfo();
            long lastExecuteAt = lastSchedule.value().executeAt();

            long interval = service.interval();
            long newExecuteAt = lastExecuteAt + interval;
            if (lastExecuteAt == 0) newExecuteAt = startTime + service.startTick();

            Queue<IndexWrapper<Schedule>> schedulesOfThisService = new LinkedList<>();
            while (endTime > newExecuteAt) {
                if (newExecuteAt < startTime) {
                    newExecuteAt += interval;
                    continue;
                }
                schedulesOfThisService.add(
                    new IndexWrapper<>(new Schedule(service, newExecuteAt), lastSchedule.index()));
                newExecuteAt += interval;
            }
            schedulesByServices.add(schedulesOfThisService);
        }
        return SortedQueueMerger.mergeSortedQueues(schedulesByServices);
    }

    private static void waitSchedule(IndexWrapper<Schedule> curSchedule)
        throws InterruptedException {
        long executeAt = curSchedule.value().executeAt();
        if (executeAt > System.currentTimeMillis()) {
            Thread.sleep(executeAt - System.currentTimeMillis());
        }
    }

    private void doProduce(Producer<String, String> producer, IndexWrapper<Schedule> curSchedule, int curMessageCount) {
        ServiceInfo curService = curSchedule.value().serviceInfo();
        String coreMessage = curService.serviceName() + "-" + index + "-" + curMessageCount;
        String message = messageGenerator.generate(coreMessage);

        ProducerRecord<String, String> record = new ProducerRecord<>(curService.topicName(), coreMessage, message);
        long curTime = System.currentTimeMillis();
        long curTimeNano = System.nanoTime();
        producer.send(record, callback);

        monitoringQueue.enqueue(new MonitorLog(
            "PRODUCE",
            coreMessage,
            "REQUESTED",
            curTime,
            curTimeNano
        ));
        monitorLogWriter.notifyIfNeeded();
    }

    private record IndexWrapper<T extends Comparable<T>>(
        T value, int index
    ) implements Comparable<IndexWrapper<T>> {
        @Override
        public int compareTo(IndexWrapper<T> other) {
            if (other == null) return 1;
            return this.value.compareTo(other.value);
        }
    }
}