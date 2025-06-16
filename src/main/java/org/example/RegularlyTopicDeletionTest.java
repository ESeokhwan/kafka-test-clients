package org.example;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.logging.log4j.ThreadContext;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class RegularlyTopicDeletionTest implements Runnable {

  @Getter
  @Parameters(index = "0", description = "Kafka Brokers")
  private String brokers;

  @Getter
  @Parameters(index = "1", description = "Topic Prefix")
  private String topicPrefix;

  @Getter
  @Option(names = {"-s", "--start-index"}, description = "Start index of topic names. It will be used with topicPrefix. Default: 0")
  private int startIndex = 0;

  @Getter
  @Option(names = {"-c", "--count"}, description = "Number of topics. It will be used with topicPrefix and startIndex. Default: 10")
  private int count = 10;

  @Getter
  @Option(names = {"-i", "--interval"}, description = "Interval of topic deletion in milli seconds. Default: 1000")
  private int interval = 1000;

  private long absTimestampBase;

  public RegularlyTopicDeletionTest() {
    super();
  }

  @Override
  public void run() {
    absTimestampBase = System.currentTimeMillis() * 1_000_000 - System.nanoTime();

    Properties props = createAdminClientConfig();

    AdminClient adminClient = KafkaAdminClient.create(props);
    for (int i = 0; i < count; i++) {
      String topicName = topicPrefix + (startIndex + i);
      log.info("Delete topic: {}", topicName);
      long startTimeNano = System.nanoTime();
      try {
        adminClient.deleteTopics(List.of(topicName)).all().get();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
      long endTimeNano = System.nanoTime();
      long elapsedTimeMs = (endTimeNano - startTimeNano) / 1_000_000;
      log.info("Deleted topic: {} in {} ms", topicName, elapsedTimeMs);

      try {
        Thread.sleep(Math.max(interval - (int) elapsedTimeMs, 0));
      } catch (InterruptedException e) {
        log.error("Thread interrupted during sleep", e);
        Thread.currentThread().interrupt();
      }
    }
  }

  private Properties createAdminClientConfig() {
    Properties props = new Properties();
    props.put("bootstrap.servers", this.brokers);

    return props;
  }

  public static void main(String[] args) {
    RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
    String pid = rt.getName();
    ThreadContext.put("PID", pid);

    new CommandLine(new RegularlyTopicDeletionTest())
        .execute(args);
  }
}
