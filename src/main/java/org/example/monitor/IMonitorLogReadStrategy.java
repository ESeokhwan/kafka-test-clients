package org.example.monitor;

import java.util.List;

public interface IMonitorLogReadStrategy {
  void read(List<MonitorLog> tar);
}
