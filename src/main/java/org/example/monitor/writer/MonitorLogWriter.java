package org.example.monitor.writer;

import org.example.monitor.MonitorLog;
import org.example.monitor.MonitorQueue;
import org.example.util.IMessageGenerator;

// TODO: 일정 시간동안 monitorQueue에 있는 데이터가 flush안되면 자동으로 flush해주는 기능 추가하기
public class MonitorLogWriter implements Runnable {

  private final MonitorQueue monitorQueue = MonitorQueue.getInstance();
  
  private final IMonitorLogWriteStrategy writeStrategy;

  private final IMessageGenerator messageGenerator;
  
  private final int batchSize;

  private boolean terminated = false;

  private int curWrittenCnt = 0;

  public MonitorLogWriter(IMonitorLogWriteStrategy writeStrategy, IMessageGenerator messageGenerator, int batchSize) {
    this.writeStrategy = writeStrategy;
    this.messageGenerator = messageGenerator;
    this.batchSize = batchSize;
  }

  public void gracefulShutdown() {
    terminated = true;
  }

  public void syncedWait() {
    synchronized (this) {
      try {
        wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void syncedNotify() {
    synchronized (this) {
      notify();
    }
  }

  public void notifyIfNeeded() {
    if (monitorQueue.size() >= batchSize) {
      syncedNotify();
    }
  }

  @Override
  public void run() {
    while (!(terminated && monitorQueue.isEmpty())) {
      if (!terminated && monitorQueue.isEmpty()) {
        syncedWait();
      }
      MonitorLog log = monitorQueue.dequeue();
      String messageId = messageGenerator.extractMessageId(log.getMessageId());
      writeStrategy.write(log.withMessageId(messageId));
      curWrittenCnt += 1;
      tryFlushBatch();
    }
    flushBatch();
  }

  private void tryFlushBatch() {
    if (curWrittenCnt >= batchSize) {
      flushBatch();
      if (!terminated && monitorQueue.size() < batchSize) {
        syncedWait();
      }
    }
  }

  private void flushBatch() {
    writeStrategy.commit();
    curWrittenCnt = 0;
  }

}