package org.example;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.ThreadContext;
import org.example.monitor.MonitorLog;
import org.example.monitor.MonitorQueue;
import org.example.monitor.writer.CsvMonitorLogWriteStrategy;
import org.example.monitor.writer.MonitorLogWriter;
import org.example.util.ExtractOnlyNaiveMessageAdaptor;
import org.example.util.IMessageAdaptor;

import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Slf4j
public class EarliestConsumerWithMonitor implements Runnable {

  @Getter
  @Parameters(index = "0", description = "Kafka Brokers")
  private String brokers;

  @Getter
  @Parameters(index = "1", arity = "1..*", description = "Topic Names")
  private String[] topics;

  @Getter
  @Option(names = {"-s", "--record-size"}, description = "Size of a single record(byte)")
  private int messageSize = 1000;

  @Getter
  @Option(names = {"-t", "--runtime"}, description = "Total runtime(sec) of this consumer. It will run eternally, when this value is set 0. Default: 1800 sec")
  private long totalRuntime = 30 * 60;

  @Getter
  @Option(names = {"-n", "--num-record"}, description = "The number of records")
  private int numRecord = 100_000;

  @Getter
  @Option(names = {"-m", "--monitor-file"}, description = "path of monitoring output file")
  private String monitorFilePath = "output/consumer_monitor_earliest.csv";

  @Getter
  @Option(names = {"-b", "--monitor-batch-size"}, description = "write batch size of monitor log")
  private int monitorBatchSize = 10_000;

  private final MonitorQueue monitoringQueue = MonitorQueue.getInstance();  

  private MonitorLogWriter monitorLogWriter;

  private IMessageAdaptor messageAdaptor;

  private long absTimestampBase;

  public EarliestConsumerWithMonitor() {
    super();
  }

  @Override
  public void run() {
    absTimestampBase = System.currentTimeMillis() * 1_000_000 - System.nanoTime();

    Properties props = createConsumerConfig();
    Thread monitorLogWriterThread = setupMonitorLogWriterThread();
    monitorLogWriterThread.start();
    
    try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(topics));

      int consumeCnt = 0;
      long expiredTime = System.nanoTime() + totalRuntime * 1000 * 1000 * 1000;
      while (totalRuntime == 0 || System.nanoTime() < expiredTime) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        long curTime = System.nanoTime() + absTimestampBase;
        log.debug("fetch {} records.", records.count());
        for (var record: records) {
          log.trace("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
          monitoringQueue.enqueue(new MonitorLog(MonitorLog.RequestType.CONSUME, record.value(), curTime, MonitorLog.State.RESPONDED));
          monitorLogWriter.notifyIfNeeded();
          consumeCnt += 1;
        }

        if (consumeCnt >= numRecord) {
          break;
        }
      }
    }
    monitorLogWriter.gracefulShutdown();
    monitorLogWriter.syncedNotify();
    
    try {
      monitorLogWriterThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private Properties createConsumerConfig() {
    UUID uuid = UUID.randomUUID();

    Properties props = new Properties();
    props.put("bootstrap.servers", this.brokers);
    props.put("group.id", uuid.toString());
    props.put("enable.auto.commit", "true");
    props.put("auto.offset.reset", "earliest");

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    return props;
  }

  private Thread setupMonitorLogWriterThread() {
    messageAdaptor = new ExtractOnlyNaiveMessageAdaptor(messageSize);
    monitorLogWriter = new MonitorLogWriter(
        new CsvMonitorLogWriteStrategy(monitorFilePath),
        messageAdaptor,
        monitorBatchSize
    );
    return new Thread(monitorLogWriter);
  }

  public static void main(String[] args) {
    RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
    String pid = rt.getName();
    ThreadContext.put("PID", pid);

    new CommandLine(new EarliestConsumerWithMonitor())
        .execute(args);
  }
}
