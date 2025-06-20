package org.example.core;

public record ServiceInfo(
    String serviceName,
    String topicName,
    long messageSize,
    long interval,
    long startTick
){
}
