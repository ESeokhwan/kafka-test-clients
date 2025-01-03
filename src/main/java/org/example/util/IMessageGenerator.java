package org.example.util;

public interface IMessageGenerator {
  String generate(String messageId);

  String extractMessageId(String message);
}
