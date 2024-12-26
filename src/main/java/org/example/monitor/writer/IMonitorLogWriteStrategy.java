package org.example.monitor.writer;

import org.example.monitor.MonitorLog;

public interface IMonitorLogWriteStrategy {
  void write(MonitorLog log);
  boolean commit();
}
