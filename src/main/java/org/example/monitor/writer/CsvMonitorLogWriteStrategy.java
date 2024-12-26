package org.example.monitor.writer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.example.monitor.MonitorLog;

public class CsvMonitorLogWriteStrategy implements IMonitorLogWriteStrategy {

  private final String filepath;

  private BufferedWriter writer;

  public CsvMonitorLogWriteStrategy(String filepath) {
    this.filepath = filepath;
  }

  @Override
  public void write(MonitorLog log) {
    if (writer == null) {
      try {
        this.writer = new BufferedWriter(new FileWriter(filepath));
        writer.append("RequestType,MessageId,Timestamp,state\n");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    try {
      writer.append(log.getType().name())
          .append(",")
          .append(log.getMessageId())
          .append(",")
          .append(String.valueOf(log.getTimestamp()))
          .append(",")
          .append(log.getState().name())
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
