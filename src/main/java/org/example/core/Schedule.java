package org.example.core;

public record Schedule(ServiceInfo serviceInfo, long executeAt) implements Comparable<Schedule> {

  @Override
  public int compareTo(Schedule oth) {
    return Long.compare(this.executeAt, oth.executeAt);
  }
}
