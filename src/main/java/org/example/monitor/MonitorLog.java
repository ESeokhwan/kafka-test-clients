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

  private final String id;

  private final State state;

  private final long timestamp;

  private final long timestampNano;
  
  public MonitorLog(RequestType type, String id, State state, long timestamp, long timestampNano) {
    this.type = type;
    this.id = id;
    this.state = state;
    this.timestamp = timestamp;
    this.timestampNano = timestampNano;
  }

  public RequestType getType() {
    return type;
  }
  
  public String getId() {
    return id;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public State getState() {
    return state;
  }

  public long getTimestampNano() {
    return timestampNano;
  }

  public MonitorLog withMessageId(String messageId) {
    return new MonitorLog(type, messageId, state, timestamp, timestampNano);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, id, state);
  }

  @Override
  public boolean equals(Object oth) {
    if (this == oth) return true;
    if (oth == null || getClass() != oth.getClass()) return false;
    
    MonitorLog converted = (MonitorLog) oth;
    return Objects.equals(this.type, converted.type)
        && Objects.equals(this.id, converted.id)
        && Objects.equals(this.state, converted.state);
  }
}
