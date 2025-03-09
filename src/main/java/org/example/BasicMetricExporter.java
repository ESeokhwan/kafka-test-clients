package org.example;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.ThreadContext;
import org.example.monitor.IMonitorLogReadStrategy;
import org.example.monitor.MessageMetric;
import org.example.monitor.MonitorLog;
import org.example.monitor.MonitorLogReadStrategy;
import org.example.monitor.writer.CsvMessageMetricWriteStrategy;
import org.example.monitor.writer.IMessageMetricWriteStrategy;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Slf4j
public class BasicMetricExporter implements Runnable {

  @Getter
  @Parameters(index = "0", description = "Path of raw data file")
  private String rawDataFilePath;

  @Getter
  @Option(names = {"-o", "--output"}, description = "Path of monitoring output file. Default: monitored_stat.csv")
  private String outputFilePath = "output/exported_metric.csv";

  private IMonitorLogReadStrategy producerMonitorLogReadStrategy;

  private IMessageMetricWriteStrategy metricWriteStrategy;

  public BasicMetricExporter() {
    super();
  }

  public static void main(String[] args) {
    RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
    String pid = rt.getName();
    ThreadContext.put("PID", pid);

    new CommandLine((new BasicMetricExporter()))
        .execute(args);

    log.info("DONE");
  }

  @Override
  public void run() {
    producerMonitorLogReadStrategy = new MonitorLogReadStrategy(rawDataFilePath);
    metricWriteStrategy = new CsvMessageMetricWriteStrategy(outputFilePath);

    List<MonitorLog> pLogs = getMonitorLogs(producerMonitorLogReadStrategy);

    HashMap<String, MonitorLog> pLogsMap = new HashMap<>();
    pLogs.stream().forEach(e -> pLogsMap.put(generateKey(e), e));

    List<MessageMetric> output = new ArrayList<>();
    List<MonitorLog> pLogMapValuesCopy = new ArrayList<>(pLogsMap.values());
    for (MonitorLog pLog: pLogMapValuesCopy) {
      String curKey = generateKey(pLog);
      if (!pLogsMap.containsKey(curKey)) continue;

      pLogsMap.remove(curKey);
      if (pLog.getState() == MonitorLog.State.REQUESTED) {
        MonitorLog pRespondedLog = pLogsMap.remove(respondedKey(pLog));
        output.add(new MessageMetric(
          pLog.getMessageId(), 
          pLog != null ? pLog.getTimestamp() : 0,
          pRespondedLog != null ? pRespondedLog.getTimestamp() : 0,
          0,
          0
        ));
        continue;
      } 
      if(pLog.getState() == MonitorLog.State.RESPONDED) {
        MonitorLog pRequestedLog = pLogsMap.remove(requestedKey(pLog));
        output.add(new MessageMetric(
          pLog.getMessageId(),
          pRequestedLog != null ? pRequestedLog.getTimestamp() : 0,
          pLog != null ? pLog.getTimestamp() : 0,
          0,
          0
        ));
        continue;
      }
    }


    Collections.sort(output, Comparator.comparingLong(MessageMetric::getProduceRequestedAt));
    // write output
    for (MessageMetric metric: output) {
      metricWriteStrategy.write(metric);
    }
    metricWriteStrategy.commit();
  }

  private List<MonitorLog> getMonitorLogs(IMonitorLogReadStrategy strategy) {
    List<MonitorLog> monitorLogs = new LinkedList<>();
    strategy.read(monitorLogs);
    return monitorLogs;
  }

  private String generateKey(MonitorLog log) {
    return log.getMessageId() + "-" + log.getType() + "-" + log.getState();
  }

  private String requestedKey(MonitorLog log) {
    return log.getMessageId() + "-" + log.getType() + "-" + MonitorLog.State.REQUESTED;
  }

  private String respondedKey(MonitorLog log) {
    return log.getMessageId() + "-" + log.getType() + "-" + MonitorLog.State.RESPONDED;
  }
}
