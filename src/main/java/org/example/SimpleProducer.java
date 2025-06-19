package org.example;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import moniq.MonitorLog;
import moniq.MonitorQueue;
import moniq.util.IMessageAdaptor;
import moniq.writer.MonitorLogWriter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.ThreadContext;

import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Slf4j
public class SimpleProducer implements Runnable {

  @Getter
  @Parameters(index = "0", description = "Kafka Brokers")
  private String brokers;

  @Getter
  @Parameters(index = "1", description = "Prefix Topic Name")
  private String topicPrefix;

  @Getter
  @Parameters(index = "2", description = "Prefix of Client ID")
  private String clientIdPrefix = "simple-producer";

  @Getter
  @Option(names = {"-a", "--is-async"}, description = "Whether each producer works async or not")
  private boolean isAsync = false;

  @Getter
  @Option(names = {"-a", "--is-topic-shared"}, description = "Whether each producer shares topic for same service or not")
  private boolean isTopicShared = false;

  @Getter
  @Option(names = {"-c", "--producer-count"}, description = "Number of producers")
  private int producerCount = 1;

  @Getter
  @Option(names = {"-C", "--service-count"}, description = "Number of services")
  private int serviceCount = 1;

  @Getter
  @Option(names = {"-i", "--interval"}, description = "Interval between each produce (ms)")
  private int interval = 0;

  @Getter
  @Option(names = {"-s", "--record-size"}, description = "Size of a single record(byte)")
  private int messageSize = 1000;

  @Getter
  @Option(names = {"-n", "--num-round"}, description = "The number of rounds of each producer and service")
  private int numRecord = 100_000;

  @Getter
  @Option(names = {"-n", "--total-record"}, description = "The total number of records to produce")
  private int totalRecord = 100_000;

  @Getter
  @Option(names = {"-t", "--running-time"}, description = "Running time of producer (ms)")
  private int runningTime = 100_000;

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

  public SimpleProducer() {
    super();
  }

  public static void main(String[] args) {
    RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
    String pid = rt.getName();
    ThreadContext.put("PID", pid);

    new CommandLine((new SimpleProducer()))
        .execute(args);

    log.info("DONE");
  }

  @Override
  public void run() {
    absTimestampBase = System.currentTimeMillis() * 1_000_000 - System.nanoTime();

    Properties props = createProducerConfig();
    Thread monitorLogWriteThread = setupMonitorLogWriterThread();

    ackCounter.set(numRecord);
    monitorLogWriteThread.start();

    for (int i = 0; i < producerCount; i++) {

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

  public class ProducerThread implements Runnable {

    private final Properties props;
    private final int index;
    private final List<ServiceInfo> services;

    private final Boolean startFlag;
    private final Boolean endFlag;

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
          long startTimeNano = System.nanoTime() + absTimestampBase;
          for (int i = 0; i < services.size(); i++) {
            ServiceInfo service = services.get(i);
            if (tick % service.interval() != 0) continue;
            String coreMessage = service.topicName() + "-" + serviceCounts.get(i);
            String message = messageGenerator.generate(coreMessage);

            ProducerRecord<String, String> record = new ProducerRecord<>(service.topicName(), coreMessage, message);
            long curTime = System.currentTimeMillis();
            long curTimeNano = System.nanoTime() + absTimestampBase;
            producer.send(record, new BasicProducerCallback(record));

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

      long curTimeNano = System.nanoTime() + absTimestampBase;
      long curTime = System.currentTimeMillis();
      String messageId = record.value();
      monitoringQueue.enqueue(new MonitorLog(
          MonitorLog.RequestType.PRODUCE, 
          messageId, MonitorLog.State.RESPONDED,
          curTime, curTimeNano
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
