package org.example.monitor;

import lombok.Getter;

public class MessageMetric {
  
  @Getter
  private String messageId;

  @Getter
  private long produceRequestedAt;

  @Getter
  private long produceRespondedAt;

  @Getter
  private long consumedAt;

  public MessageMetric(String messageId, long produceRequestedAt, long produceRespondedAt, long consumedAt) {
    this.messageId = messageId;
    this.produceRequestedAt = produceRequestedAt;
    this.produceRespondedAt = produceRespondedAt;
    this.consumedAt = consumedAt;
  }

  public long getE2ELatency() {
    return consumedAt - produceRequestedAt;
  }

  public long getProduceLatency() {
    return produceRespondedAt - produceRequestedAt;
  }

  public MessageMetric withMessageId(String messageId) {
    return new MessageMetric(messageId, produceRequestedAt, produceRespondedAt, consumedAt);
  }

  public MessageMetric withProduceRequestedAt(long produceRequestedAt) {
    return new MessageMetric(messageId, produceRequestedAt, produceRespondedAt, consumedAt);
  }

  public MessageMetric withProduceRespondedAt(long produceRespondedAt) {
    return new MessageMetric(messageId, produceRequestedAt, produceRespondedAt, consumedAt);
  }

  public MessageMetric withConsumedAt(long consumedAt) {
    return new MessageMetric(messageId, produceRequestedAt, produceRespondedAt, consumedAt);
  }
}
