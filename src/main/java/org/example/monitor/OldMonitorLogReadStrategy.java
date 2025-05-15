package org.example.monitor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class OldMonitorLogReadStrategy implements IMonitorLogReadStrategy {
  
  private final String filepath;

  private BufferedReader reader;

  public OldMonitorLogReadStrategy(String filepath) {
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
    if (splittedStr.length != 4) {
      return null;
    }

    try {
      MonitorLog.RequestType type = MonitorLog.RequestType.valueOf(splittedStr[0]);
      String messageId = splittedStr[1];
      long timestamp = Long.parseLong(splittedStr[2]);
      MonitorLog.State state = MonitorLog.State.valueOf(splittedStr[3]);
      return new MonitorLog(type, messageId, state, timestamp, 0);
    } catch (Exception e) {
      return null;
    }
  }
}
