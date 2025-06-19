package org.example;

import moniq.MonitorLog;
import moniq.MonitorQueue;
import moniq.writer.MonitorLogWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.util.IMessageAdaptor;

import java.util.*;

public class ProducerThread implements Runnable {

    private final Properties props;
    private final int index;
    private final List<ServiceInfo> services;

    private final Boolean startFlag;
    private final Boolean endFlag;

    private final IMessageAdaptor messageGenerator;
    private final MonitorLogWriter monitorLogWriter;
    private final MonitorQueue monitoringQueue;

    public ProducerThread(Properties props, int index, List<ServiceInfo> services) {
        this.props = props;
        this.index = index;
        this.services = services;
    }


    @Override
    public void run() {
        int tick = 0;
        List<Integer> serviceCounts = new ArrayList<>();
        for (ServiceInfo service : services) {
            serviceCounts.add(0);
        }

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            while (!endFlag) {
                long startTimeNano = System.nanoTime();
                for (int i = 0; i < services.size(); i++) {
                    ServiceInfo service = services.get(i);
                    if (tick % service.interval() != 0) continue;
                    String coreMessage = service.topicName() + "-" + serviceCounts.get(i);
                    String message = messageGenerator.generate(coreMessage);

                    ProducerRecord<String, String> record = new ProducerRecord<>(service.topicName(), coreMessage, message);
                    long curTime = System.currentTimeMillis();
                    long curTimeNano = System.nanoTime();
                    producer.send(record, new SimpleProducer.BasicProducerCallback(record));

                    monitoringQueue.enqueue(new MonitorLog(
                            "PRODUCE",
                            coreMessage,
                            "REQUESTED",
                            curTime,
                            curTimeNano
                    ));
                    monitorLogWriter.notifyIfNeeded();
                }
                long elapsedTime = System.nanoTime() - startTimeNano;

            }

        } catch (Exception e) {
            log.error("Error in sending record {}", e);
        }
    }

    private Queue<Long> getServiceSchedules(int size, List<ServiceInfo>) {
        Queue<Long> serviceSchedules = new ArrayDeque<>(size);
        for (ServiceInfo service: services) {
            serviceSchedules.add(service.interval());
        }
        return serviceSchedules;
    }
}