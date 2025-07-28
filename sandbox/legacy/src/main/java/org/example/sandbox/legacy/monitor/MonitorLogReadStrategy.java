package org.example.sandbox.legacy.monitor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class MonitorLogReadStrategy implements IMonitorLogReadStrategy {
  
  private final String filepath;

  private BufferedReader reader;

  public MonitorLogReadStrategy(String filepath) {
    this.filepath = filepath;
  }

  private void tryInitReader() {
    if (reader != null) return;
    try {
      reader = new BufferedReader(new FileReader(filepath));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void read(List<MonitorLog> tar) {
    tryInitReader();

    reader.lines().skip(1).forEach(e -> {
      tar.add(parseMonitorLog(e));
    });
  }

  private MonitorLog parseMonitorLog(String str) {
    String[] splittedStr = str.split(",");
    if (splittedStr.length != 5) {
      return null;
    }

    try {
      MonitorLog.RequestType type = MonitorLog.RequestType.valueOf(splittedStr[0]);
      String id = splittedStr[1];
      MonitorLog.State state = MonitorLog.State.valueOf(splittedStr[2]);
      long timestamp = Long.parseLong(splittedStr[3]);
      long timestampNano = Long.parseLong(splittedStr[4]);
      return new MonitorLog(type, id, state, timestamp, timestampNano);
    } catch (Exception e) {
      return null;
    }
  }
}
