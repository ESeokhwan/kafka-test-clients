package org.example;

public record ServiceInfo(
    String serviceName,
    String topicName,
    long messageSize,
    long interval,
    long startTick
){
}
