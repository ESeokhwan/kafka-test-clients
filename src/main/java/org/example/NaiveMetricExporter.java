package org.example;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
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
public class NaiveMetricExporter implements Runnable {

  @Getter
  @Parameters(index = "0", description = "Path of produce request data file")
  private String produceFilePath;

  @Getter
  @Parameters(index = "1", description = "Path of consume request data file")
  private String consumeFilePath;

  @Getter
  @Option(names = {"-o", "--output"}, description = "Path of monitoring output file. Default: monitored_stat.csv")
  private String outputFilePath = "output/exported_metric.csv";

  private IMonitorLogReadStrategy producerMonitorLogReadStrategy;

  private IMonitorLogReadStrategy consumerMonitorLogReadStrategy;

  private IMessageMetricWriteStrategy metricWriteStrategy;

  public NaiveMetricExporter() {
    super();
  }

  public static void main(String[] args) {
    RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
    String pid = rt.getName();
    ThreadContext.put("PID", pid);

    new CommandLine((new NaiveMetricExporter()))
        .execute(args);

    log.info("DONE");
  }

  @Override
  public void run() {
    producerMonitorLogReadStrategy = new MonitorLogReadStrategy(produceFilePath);
    consumerMonitorLogReadStrategy = new MonitorLogReadStrategy(consumeFilePath);
    metricWriteStrategy = new CsvMessageMetricWriteStrategy(outputFilePath);

    List<MonitorLog> pLogs = getMonitorLogs(producerMonitorLogReadStrategy);
    List<MonitorLog> cLogs = getMonitorLogs(consumerMonitorLogReadStrategy);

    HashMap<String, MonitorLog> pLogsMap = new HashMap<>();
    pLogs.stream().forEach(e -> pLogsMap.put(generateKey(e), e));

    List<MessageMetric> output = new ArrayList<>();
    for (MonitorLog cLog: cLogs) {
      MonitorLog pRequestedLog = pLogsMap.remove(pRequestedKey(cLog));
      MonitorLog pRespondedLog = pLogsMap.remove(pRespondedKey(cLog));

      output.add(new MessageMetric(
          cLog.getMessageId(), 
          pRequestedLog != null ? pRequestedLog.getTimestamp() : 0,
          pRespondedLog != null ? pRespondedLog.getTimestamp() : 0,
          cLog != null ? cLog.getTimestamp() : 0
      ));
    }

    for (MonitorLog pLog: pLogsMap.values()) {
      String curKey = generateKey(pLog);
      if (!pLogsMap.containsKey(curKey)) continue;

      pLogsMap.remove(curKey);
      if (pLog.getState() == MonitorLog.State.REQUESTED) {
        MonitorLog pRespondedLog = pLogsMap.remove(pRespondedKey(pLog));
        output.add(new MessageMetric(
          pLog.getMessageId(), 
          pLog != null ? pLog.getTimestamp() : 0,
          pRespondedLog != null ? pRespondedLog.getTimestamp() : 0,
          0
        ));
        continue;
      } 
      if(pLog.getState() == MonitorLog.State.RESPONDED) {
        MonitorLog pRequestedLog = pLogsMap.remove(pRequestedKey(pLog));
        output.add(new MessageMetric(
          pLog.getMessageId(),
          pRequestedLog != null ? pRequestedLog.getTimestamp() : 0,
          pLog != null ? pLog.getTimestamp() : 0,
          0
        ));
        continue;
      }
    }

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

  private String pRequestedKey(MonitorLog log) {
    return log.getMessageId() + "-" + MonitorLog.RequestType.PRODUCE + "-" + MonitorLog.State.REQUESTED;
  }

  private String pRespondedKey(MonitorLog log) {
    return log.getMessageId() + "-" + MonitorLog.RequestType.PRODUCE + "-" + MonitorLog.State.RESPONDED;
  }
}
