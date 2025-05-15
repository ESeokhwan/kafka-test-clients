package org.example.monitor;

import lombok.Getter;

public class MessageMetric {
  
  @Getter
  private String id;

  @Getter
  private long produceRequestedAt;

  @Getter
  private long produceRespondedAt;

  @Getter
  private long consumeRequestedAt;

  @Getter
  private long consumeRespondedAt;

  @Getter
  private long produceRequestedAtNano;

  @Getter
  private long produceRespondedAtNano;

  @Getter
  private long consumeRequestedAtNano;

  @Getter
  private long consumeRespondedAtNano;

  public MessageMetric(String id, long produceRequestedAt, long produceRespondedAt, long consumeRequestedAt, long consumeRespondedAt, long produceRequestedAtNano, long produceRespondedAtNano, long consumeRequestedAtNano, long consumeRespondedAtNano) {
    this.id = id;
    this.produceRequestedAt = produceRequestedAt;
    this.produceRespondedAt = produceRespondedAt;
    this.consumeRequestedAt = consumeRequestedAt;
    this.consumeRespondedAt = consumeRespondedAt;
    this.produceRequestedAtNano = produceRequestedAtNano;
    this.produceRespondedAtNano = produceRespondedAtNano;
    this.consumeRequestedAtNano = consumeRequestedAtNano;
    this.consumeRespondedAtNano = consumeRespondedAtNano;
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

  public long getE2ELatencyNano() {
    return consumeRespondedAtNano - produceRequestedAtNano;
  }

  public long getProduceLatencyNano() {
    return produceRespondedAtNano - produceRequestedAtNano;
  }

  public long getConsumeLatencyNano() {
    return consumeRespondedAtNano - consumeRequestedAtNano;
  }

  public MessageMetric withId(String messageId) {
    return new MessageMetric(messageId, produceRequestedAt, produceRespondedAt, consumeRequestedAt, consumeRespondedAt, produceRequestedAtNano, produceRespondedAtNano, consumeRequestedAtNano, consumeRespondedAtNano);
  }

  public MessageMetric withProduceRequestedAt(long produceRequestedAt) {
    return new MessageMetric(id, produceRequestedAt, produceRespondedAt, consumeRequestedAt, consumeRespondedAt, produceRequestedAtNano, produceRespondedAtNano, consumeRequestedAtNano, consumeRespondedAtNano);
  }

  public MessageMetric withProduceRespondedAt(long produceRespondedAt) {
    return new MessageMetric(id, produceRequestedAt, produceRespondedAt, consumeRequestedAt, consumeRespondedAt, produceRequestedAtNano, produceRespondedAtNano, consumeRequestedAtNano, consumeRespondedAtNano);
  }

  public MessageMetric withConsumeRequestedAt(long consumeRequestedAt) {
    return new MessageMetric(id, produceRequestedAt, produceRespondedAt, consumeRequestedAt, consumeRespondedAt, produceRequestedAtNano, produceRespondedAtNano, consumeRequestedAtNano, consumeRespondedAtNano);
  }

  public MessageMetric withConsumeRespondedAt(long consumeRespondedAt) {
    return new MessageMetric(id, produceRequestedAt, produceRespondedAt, consumeRequestedAt, consumeRespondedAt, produceRequestedAtNano, produceRespondedAtNano, consumeRequestedAtNano, consumeRespondedAtNano);
  }

  public MessageMetric withProduceRequestedAtNano(long produceRequestedAtNano) {
    return new MessageMetric(id, produceRequestedAt, produceRespondedAt, consumeRequestedAt, consumeRespondedAt, produceRequestedAtNano, produceRespondedAtNano, consumeRequestedAtNano, consumeRespondedAtNano);
  }

  public MessageMetric withProduceRespondedAtNano(long produceRespondedAtNano) {
    return new MessageMetric(id, produceRequestedAt, produceRespondedAt, consumeRequestedAt, consumeRespondedAt, produceRequestedAtNano, produceRespondedAtNano, consumeRequestedAtNano, consumeRespondedAtNano);
  }

  public MessageMetric withConsumeRequestedAtNano(long consumeRequestedAtNano) {
    return new MessageMetric(id, produceRequestedAt, produceRespondedAt, consumeRequestedAt, consumeRespondedAt, produceRequestedAtNano, produceRespondedAtNano, consumeRequestedAtNano, consumeRespondedAtNano);
  }

  public MessageMetric withConsumeRespondedAtNano(long consumeRespondedAtNano) {
    return new MessageMetric(id, produceRequestedAt, produceRespondedAt, consumeRequestedAt, consumeRespondedAt, produceRequestedAtNano, produceRespondedAtNano, consumeRequestedAtNano, consumeRespondedAtNano);
  }
}
