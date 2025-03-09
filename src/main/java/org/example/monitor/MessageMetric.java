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
  private long consumeRequestedAt;

  @Getter
  private long consumeRespondedAt;

  public MessageMetric(String messageId, long produceRequestedAt, long produceRespondedAt, long consumeRequestedAt ,long consumeRespondedAt) {
    this.messageId = messageId;
    this.produceRequestedAt = produceRequestedAt;
    this.produceRespondedAt = produceRespondedAt;
    this.consumeRequestedAt = consumeRequestedAt;
    this.consumeRespondedAt = consumeRespondedAt;
  }

  public long getE2ELatency() {
    return consumeRespondedAt - produceRequestedAt;
  }

  public long getProduceLatency() {
    return produceRespondedAt - produceRequestedAt;
  }

  public long getConsumeLatency() {
    return consumeRespondedAt - consumeRequestedAt;
  }

  public MessageMetric withMessageId(String messageId) {
    return new MessageMetric(messageId, produceRequestedAt, produceRespondedAt, consumeRequestedAt, consumeRespondedAt);
  }

  public MessageMetric withProduceRequestedAt(long produceRequestedAt) {
    return new MessageMetric(messageId, produceRequestedAt, produceRespondedAt, consumeRequestedAt, consumeRespondedAt);
  }

  public MessageMetric withProduceRespondedAt(long produceRespondedAt) {
    return new MessageMetric(messageId, produceRequestedAt, produceRespondedAt, consumeRequestedAt, consumeRespondedAt);
  }

  public MessageMetric withConsumedAt(long consumedAt) {
    return new MessageMetric(messageId, produceRequestedAt, produceRespondedAt, consumeRequestedAt, consumeRespondedAt);
  }
}
