package org.example;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.ThreadContext;
import org.example.monitor.MonitorLog;
import org.example.monitor.MonitorQueue;
import org.example.monitor.writer.CsvMonitorLogWriteStrategy;
import org.example.monitor.writer.MonitorLogWriter;

import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Slf4j
public class BasicProducerWithMonitor implements Runnable {

  @Getter
  @Parameters(index = "0", description = "Kafka Brokers")
  private String brokers;

  @Getter
  @Parameters(index = "1", description = "Topic Name")
  private String topicName;

  @Getter
  @Option(names = {"-k", "--key"},description = "producer key", defaultValue = "test")
  private String producerKey = "BasicProducer";

  @Getter
  @Option(names = {"-s", "--size"}, description = "end Page Num")
  private int dataSize = 100_000;

  @Getter
  @Option(names = {"-m", "--monitor-file"}, description = "path of monitoring output file")
  private String monitorFilePath = "output/producer_monitor.csv";

  @Getter
  @Option(names = {"-b", "--monitor-batch-size"}, description = "write batch size of monitor log")
  private int monitorBatchSize = 10_000;

  private final MonitorQueue monitoringQueue = MonitorQueue.getInstance();

  private MonitorLogWriter monitorLogWriter;

  private AtomicInteger ackCounter = new AtomicInteger();

  public BasicProducerWithMonitor() {
    super();
  }

  public static void main(String[] args) {
    RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
    String pid = rt.getName();
    ThreadContext.put("PID", pid);

    new CommandLine((new BasicProducerWithMonitor()))
        .execute(args);

    log.info("DONE");
  }

  @Override
  public void run() {
    if (this.producerKey.contains("-")) {
      log.error("producerKey should not contain '-'");
      return;
    }

    Properties props = createProducerConfig();
    Thread monitorLogWriteThread = setupMonitorLogWriterThread();

    ackCounter.set(dataSize);
    monitorLogWriteThread.start();

    try (Producer<String, String> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < dataSize; i++) {
        String messageId = this.producerKey + "-" + String.valueOf(i + 1);
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, messageId);
        long requestedTime = System.nanoTime();
        producer.send(record, new BasicProducerCallback(record));
        monitoringQueue.enqueue(new MonitorLog(
            MonitorLog.RequestType.PRODUCE, 
            messageId, requestedTime, 
            MonitorLog.State.REQUESTED
        ));
        monitorLogWriter.notifyIfNeeded();
      }
    } catch (Exception e) {
      log.error("Error in sending record {}", e);
    }
    
    try {
      monitorLogWriteThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private Properties createProducerConfig() {
    Properties props = new Properties();
    props.put("bootstrap.servers", this.brokers);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("client.id", "basic-producer-with-monitor");

    return props;
  }

  private Thread setupMonitorLogWriterThread() {
    monitorLogWriter = new MonitorLogWriter(
        new CsvMonitorLogWriteStrategy(monitorFilePath),
        monitorBatchSize
    );
    return new Thread(monitorLogWriter);
  }

  public class BasicProducerCallback implements Callback {

    private final ProducerRecord<String, String> record;

    public BasicProducerCallback(ProducerRecord<String, String> record) {
      this.record = record;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if (exception != null) {
        exception.printStackTrace();
        return;
      }
      long respondedTime = System.nanoTime();

      String messageId = record.value();
      monitoringQueue.enqueue(new MonitorLog(
          MonitorLog.RequestType.PRODUCE, 
          messageId, respondedTime,
          MonitorLog.State.RESPONDED
      ));
      monitorLogWriter.notifyIfNeeded();

      // TODO: 기본적으로는 이 방식으로 종료를 하는데, 만약 일정 시간이 지났는데도 
      //       모든 request에 대한 응답이 오지 않는다면 바로 종료되는 기능 추가하기
      int curCounter = ackCounter.decrementAndGet();
      if (curCounter == 0) {
        log.info("process all ack");
        monitorLogWriter.gracefulShutdown();
        monitorLogWriter.syncedNotify();
      }
    }
  }
}
