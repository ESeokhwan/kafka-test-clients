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
        writer.append("message id,produceRequested at,produceResponded at,consumed at,E2E Latency,Producer Latency\n");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    try {
      writer.append(metric.getMessageId())
          .append(",")
          .append(String.valueOf((double) metric.getProduceRequestedAt() / 1_000_000.0))
          .append(",")
          .append(String.valueOf((double) metric.getProduceRespondedAt() / 1_000_000.0))
          .append(",")
          .append(String.valueOf((double) metric.getConsumedAt() / 1_000_000.0))
          .append(",")
          .append(String.valueOf((double) metric.getE2ELatency() / 1_000_000.0))
          .append(",")
          .append(String.valueOf((double) metric.getProduceLatency() / 1_000_000.0))
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
