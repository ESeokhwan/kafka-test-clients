package org.example;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.ThreadContext;
import org.example.monitor.MonitorLog;
import org.example.monitor.MonitorLog.RequestType;
import org.example.monitor.MonitorLog.State;
import org.example.monitor.MonitorQueue;
import org.example.monitor.writer.CsvMonitorLogWriteStrategy;
import org.example.monitor.writer.MonitorLogWriter;
import org.example.util.ExtractOnlyNaiveMessageAdaptor;
import org.example.util.IMessageAdaptor;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MultiThreadEarliestConsumerWithMonitor implements Runnable {

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
  @Option(names = {"-p", "--partition-count"}, description = "Count of Partitions")
  private int partitionCnt = 1;

  @Getter
  @Option(names = {"-t", "--runtime"}, description = "Total runtime(sec) of this consumer. It will run eternally, when this value is set 0. Default: 1800 sec")
  private long totalRuntime = 30 * 60;

  @Getter
  @Option(names = {"-n", "--num-record"}, description = "The number of records")
  private int numRecord = 100_000;

  @Getter
  @Option(
      names = {"-r", "--refresh-interval"},
      description = "Offset Reset interval. This value is not exact refresh interval, "
          + "offset refresh is occurred when number of fetched data from last offset reset has exceeded this value. "
          + "It will never reset offset, when this value is set 0. Default: 0"
  )
  private int refreshInterval = 0;

  @Getter
  @Option(names = {"-m", "--monitor-file"}, description = "path of monitoring output file")
  private String monitorFilePath = "output/consumer_monitor_earliest.csv";

  @Getter
  @Option(names = {"-b", "--monitor-batch-size"}, description = "write batch size of monitor log")
  private int monitorBatchSize = 10_000;

  private final MonitorQueue monitoringQueue = MonitorQueue.getInstance();

  private MonitorLogWriter monitorLogWriter;

  private AtomicInteger ackCounter = new AtomicInteger();

  private IMessageAdaptor messageAdaptor;

  private long absTimestampBase;

  public MultiThreadEarliestConsumerWithMonitor() {
    super();
  }

  public static void main(String[] args) {
    RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
    String pid = rt.getName();
    ThreadContext.put("PID", pid);

    new CommandLine(new MultiThreadEarliestConsumerWithMonitor())
            .execute(args);
  }

  @Override
  public void run() {
    absTimestampBase = System.currentTimeMillis() * 1_000_000 - System.nanoTime();

    Properties props = createConsumerConfig();
    Thread monitorLogWriterThread = setupMonitorLogWriterThread();
    monitorLogWriterThread.start();

    ackCounter.set(numRecord);

    List<Thread> consumerThreads = new ArrayList<>();
    for (int i = 0; i < partitionCnt; i++) {
      Thread thread = new Thread(new ConsumerThread(props, i));
      consumerThreads.add(thread);
      thread.start();
    }

    for (int i = 0; i < partitionCnt; i++) {
      try {
        consumerThreads.get(i).join();
      } catch (InterruptedException e) {
        e.printStackTrace();;
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

  public class ConsumerThread implements Runnable {

    private final Properties props;
    private final int partitionNum;

    public ConsumerThread(Properties props, int partitionNum) {
      this.props = props;
      this.partitionNum = partitionNum;
    }

    @Override
    public void run() {
      try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
        consumer.assign(Arrays.stream(topics).map(t -> new TopicPartition(t, partitionNum)).toList());

        int consumeCnt = 0;
        int intervalConsumeCnt = 0;
        int partedRefreshInterval = refreshInterval / partitionCnt;
        long expiredTime = System.nanoTime() + totalRuntime * 1000 * 1000 * 1000;
        while (totalRuntime == 0 || System.nanoTime() < expiredTime) {
          if (partedRefreshInterval > 0 && intervalConsumeCnt > partedRefreshInterval) {
            consumer.seekToBeginning(Arrays.stream(topics).map(topic -> new TopicPartition(topic, partitionNum)).toList());
            intervalConsumeCnt = 0;
          }
          monitoringQueue.enqueue(new MonitorLog(RequestType.CONSUME, "fetch-" + partitionNum + "-" + consumeCnt, System.nanoTime() + absTimestampBase, State.REQUESTED));
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
          monitoringQueue.enqueue(new MonitorLog(RequestType.CONSUME, "fetch-" + partitionNum + "-" + consumeCnt, System.nanoTime() + absTimestampBase, State.RESPONDED));
          monitorLogWriter.notifyIfNeeded();
          log.debug("fetch {} records.", records.count());
          for (var record: records) {
            log.trace("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
            ackCounter.decrementAndGet();
            consumeCnt += 1;
            intervalConsumeCnt += 1;
          }
          int curCounter = ackCounter.get();

          if (curCounter == 0) {
            log.info("process all fetch request");
            break;
          }
        }
      }
    }
  }
}
