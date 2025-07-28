package org.example.sandbox.legacy.util;

public interface IMessageAdaptor {
  String generate(String messageId);

  String extractMessageId(String message);
}
