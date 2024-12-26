package org.example.monitor.writer;

import org.example.monitor.MessageMetric;

public interface IMessageMetricWriteStrategy {
  void write(MessageMetric log);
  boolean commit();
}
