package org.example.sandbox.legacy.monitor.writer;

import org.example.sandbox.legacy.monitor.MessageMetric;

public interface IMessageMetricWriteStrategy {
  void write(MessageMetric log);
  boolean commit();
}
