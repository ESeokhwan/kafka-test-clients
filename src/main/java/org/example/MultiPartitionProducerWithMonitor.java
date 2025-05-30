package org.example;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.logging.log4j.ThreadContext;
import org.example.monitor.MonitorLog;
import org.example.monitor.MonitorQueue;
import org.example.monitor.writer.CsvMonitorLogWriteStrategy;
import org.example.monitor.writer.MonitorLogWriter;
import org.example.util.EfficientMessageGenerator;
import org.example.util.IMessageAdaptor;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MultiPartitionProducerWithMonitor implements Runnable {

  @Getter
  @Parameters(index = "0", description = "Kafka Brokers")
  private String brokers;

  @Getter
  @Parameters(index = "1", description = "Topic Name")
  private String topicName;

  @Getter
  @Option(names = {"-a", "--is-async"}, description = "Whether this producer work async or not")
  private boolean isAsync = false;

  @Getter
  @Option(names = {"-k", "--key"},description = "producer key", defaultValue = "test")
  private String producerKey = "BasicProducer";

  @Getter
  @Option(names = {"-p", "--partition-count"}, description = "The number of partitions")
  private int partitionCount = 100;

  @Getter
  @Option(names = {"-n", "--num-record"}, description = "The number of records in partition")
  private int numRecord = 10_000;

  @Getter
  @Option(names = {"-s", "--record-size"}, description = "Size of a single record(byte)")
  private int messageSize = 10_000;

  @Getter
  @Option(names = {"-m", "--monitor-file"}, description = "path of monitoring output file")
  private String monitorFilePath = "output/producer_monitor.csv";

  @Getter
  @Option(names = {"-b", "--monitor-batch-size"}, description = "write batch size of monitor log")
  private int monitorBatchSize = 10_000;

  private final MonitorQueue monitoringQueue = MonitorQueue.getInstance();

  private MonitorLogWriter monitorLogWriter;

  private AtomicInteger ackCounter = new AtomicInteger();

  private IMessageAdaptor messageGenerator;

  private long absTimestampBase;

  public MultiPartitionProducerWithMonitor() {
    super();
  }

  public static void main(String[] args) {
    RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
    String pid = rt.getName();
    ThreadContext.put("PID", pid);

    new CommandLine((new MultiPartitionProducerWithMonitor()))
        .execute(args);

    log.info("DONE");
  }

  @Override
  public void run() {
    if (this.producerKey.contains("-")) {
      log.error("producerKey should not contain '-'");
      return;
    }
    absTimestampBase = System.currentTimeMillis() * 1_000_000 - System.nanoTime();

    Properties props = createProducerConfig();
    Thread monitorLogWriteThread = setupMonitorLogWriterThread();

    int totalNumRecord = numRecord * partitionCount;
    ackCounter.set(totalNumRecord);
    monitorLogWriteThread.start();

    try (Producer<String, String> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < numRecord; i++) {
        for (int j = 0; j < partitionCount; j++) {
          String messageId = this.producerKey + "-" + j + "-" + String.valueOf(i + 1);
          String message = messageGenerator.generate(messageId);

          ProducerRecord<String, String> record = new ProducerRecord<>(topicName, j, messageId, message);

          long requestedTime = System.currentTimeMillis();
          long requestedTimeNano = System.nanoTime() + absTimestampBase;
          if (isAsync) {
            producer.send(record, new BasicProducerCallback(record));
          } else {
            producer.send(record, new BasicProducerCallback(record)).get();
          }
          monitoringQueue.enqueue(new MonitorLog(
                  MonitorLog.RequestType.PRODUCE,
                  messageId, MonitorLog.State.REQUESTED,
                  requestedTime, requestedTimeNano
          ));
          monitorLogWriter.notifyIfNeeded();
        }
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
    props.put("batch.size", "0");

    return props;
  }

  private Thread setupMonitorLogWriterThread() {
    messageGenerator = new EfficientMessageGenerator(messageSize, 100_000);
    monitorLogWriter = new MonitorLogWriter(
        new CsvMonitorLogWriteStrategy(monitorFilePath),
        messageGenerator,
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
      long respondedTimeNano = System.nanoTime() + absTimestampBase;
      long respondedTime = System.currentTimeMillis();

      String messageId = record.value();
      monitoringQueue.enqueue(new MonitorLog(
          MonitorLog.RequestType.PRODUCE, 
          messageId, MonitorLog.State.RESPONDED,
          respondedTime, respondedTimeNano
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
