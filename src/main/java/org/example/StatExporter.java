package org.example;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
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
public class StatExporter implements Runnable {

  // @Getter
  // @Parameters(index = "0", description = "Path of produce request data file")
  // private String produceFilePath;

  // @Getter
  // @Parameters(index = "1", description = "Path of consume request data file")
  // private String consumeFilePath;

  // @Getter
  // @Option(names = {"-o", "--output"}, description = "Path of monitoring output file. Default: monitored_stat.csv")
  // private String monitorFilePath = "producer_monitor.csv";

  // @Getter
  // @Option(names = {"-p", "--p-window-size"}, description = "Window size of producer monitor log. Default: 100,000")
  // private int pWindowSize = 100_000;

  // @Getter
  // @Option(names = {"-c", "--c-window-size"}, description = "Window size of consumer monitor log. Default: 100,000")
  // private int cWindowSize = 100_000;

  // private final IMonitorLogReadStrategy producerMonitorLogReadStrategy;

  // private final IMonitorLogReadStrategy consumerMonitorLogReadStrategy;

  // public StatExporter() {
  //   super();
  // }

  public static void main(String[] args) {
    RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
    String pid = rt.getName();
    ThreadContext.put("PID", pid);

    new CommandLine((new StatExporter()))
        .execute(args);

    log.info("DONE");
  }

  @Override
  public void run() {
    // LinkedList<MonitorLog> producerMonitorLogs;
    // LinkedList<MonitorLog> consumerMonitorLogs;

    // long nextTimestamp = 0L;
    // while (!(
    //     producerMonitorLogs = getMonitorLogs(producerMonitorLogReadStrategy, nextTimestamp, pWindowSize)
    // ).isEmpty()) {
    //   long cNextTimestamp = producerMonitorLogs.getFirst().getTimestamp();
    //   while (!(
    //       consumerMonitorLogs = getMonitorLogs(consumerMonitorLogReadStrategy, cNextTimestamp, cWindowSize)
    //   ).isEmpty()) {
    //     // pWindowSize x cWindowSize 만큼 돌면서 같은거 찾고 있으면 output에 추가
    //     // 찾으면 produceRequest에서 지우기

        
    //   }
    // }
  }

  // public List<MonitorLog> getMonitorLogs(IMonitorLogReadStrategy strategy, long timestamp, int windowSize) {
  //   List<MonitorLog> monitorLogs = new LinkedList<>();
  //   strategy.readAfterTimestamp(monitorLogs, 0L, pWindowSize);
  //   return monitorLogs;
  // }
}
