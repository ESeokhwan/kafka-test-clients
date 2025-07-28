package org.example.sandbox.legacy.monitor.writer;

import org.example.sandbox.legacy.monitor.MonitorLog;

public interface IMonitorLogWriteStrategy {
  void write(MonitorLog log);
  boolean commit();
}
