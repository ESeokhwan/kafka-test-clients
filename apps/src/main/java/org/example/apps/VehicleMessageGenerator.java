package org.example.apps;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import moniq.util.NaiveMessageGenerator;

/** 차량마다 한 번 만든 패딩을 재사용하며, 메시지 ID와 고정 바이트 크기를 유지한다. */
final class VehicleMessageGenerator {
    private final int messageSize;
    private final String padding;

    VehicleMessageGenerator(int messageSize) {
        if (messageSize < 1) {
            throw new IllegalArgumentException("Message size must be positive");
        }
        this.messageSize = messageSize;
        // 기존 생성기의 문자 집합과 패턴을 사용하되 가변 인덱스는 생성 시에만 사용한다.
        String template = new NaiveMessageGenerator(messageSize, Math.min(messageSize, 1000)).generate("");
        this.padding = template.substring(1);
    }

    String generate(String messageId) {
        Objects.requireNonNull(messageId, "messageId");
        // 순번 자릿수가 늘어나는 만큼 패딩을 줄여 UTF-8 직렬화 크기를 고정한다.
        int paddingLength = messageSize - messageId.getBytes(StandardCharsets.UTF_8).length - 1;
        if (paddingLength < 0) {
            throw new IllegalArgumentException("Message ID and delimiter exceed --msg-size: " + messageSize);
        }
        // 이전 payload를 덮어쓰지 않는다. async 전송마다 독립적인 불변 String을 반환한다.
        return new StringBuilder(messageSize)
                .append(messageId).append('!')
                .append(padding, 0, paddingLength)
                .toString();
    }
}
