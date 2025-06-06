package org.example.monitor.writer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.example.monitor.MessageMetric;

public class CsvMessageMetricWriteStrategy implements IMessageMetricWriteStrategy {

  private final String filepath;

  private BufferedWriter writer;

  public CsvMessageMetricWriteStrategy(String filepath) {
    this.filepath = filepath;
  }

  @Override
  public void write(MessageMetric metric) {
    if (writer == null) {
      try {
        this.writer = new BufferedWriter(new FileWriter(filepath));
        writer.append("id,produceRequestedAt,produceRespondedAt,consumeRequestedAt,consumeRespondedAt,e2eLatency,producerLatency,consumerLatency\n");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    try {
      writer.append(metric.getId())
          .append(",")
          .append(String.valueOf(metric.getProduceRequestedAt()))
          .append(",")
          .append(String.valueOf(metric.getProduceRespondedAt()))
          .append(",")
          .append(String.valueOf(metric.getConsumeRequestedAt()))
          .append(",")
          .append(String.valueOf(metric.getConsumeRespondedAt()))
          .append(",")
          .append(String.valueOf((double) metric.getE2ELatency()))
          .append(",")
          .append(String.valueOf((double) metric.getProduceLatency()))
          .append(",")
          .append(String.valueOf((double) metric.getConsumeLatency()))
          .append("\n");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public boolean commit() {
    try {
      this.writer.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return true;
  }
  
}
