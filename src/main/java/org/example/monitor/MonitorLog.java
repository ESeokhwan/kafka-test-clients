package org.example.monitor;

import java.util.Objects;

public class MonitorLog {
  public enum RequestType {
    PRODUCE, CONSUME
  }

  public enum State {
    REQUESTED, RESPONDED
  }

  private final RequestType type;

  private final String messageId;

  private final long timestamp;
  
  private final State state;
  
  public MonitorLog(RequestType type, String messageId, long timestamp, State state) {
    this.type = type;
    this.messageId = messageId;
    this.timestamp = timestamp;
    this.state = state;
  }

  public RequestType getType() {
    return type;
  }
  
  public String getMessageId() {
    return messageId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public State getState() {
    return state;
  }

  public MonitorLog withMessageId(String messageId) {
    return new MonitorLog(type, messageId, timestamp, state);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, messageId, state);
  }

  @Override
  public boolean equals(Object oth) {
    if (this == oth) return true;
    if (oth == null || getClass() != oth.getClass()) return false;
    
    MonitorLog converted = (MonitorLog) oth;
    return Objects.equals(this.type, converted.type)
        && Objects.equals(this.messageId, converted.messageId)
        && Objects.equals(this.state, converted.state);
  }
}
