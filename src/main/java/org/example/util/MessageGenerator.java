package org.example.util;

import java.util.Random;

public class MessageGenerator implements IMessageGenerator {

  private final static String paddingCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

  private final static char divChar = '!';

  private final int messageSize;

  private int[] preGeneratedIndices;

  private int curIdx;

  public MessageGenerator(int messageSize, int preIndicesSize) {
    this.messageSize = messageSize;
    init(preIndicesSize);
  }

  public String generate(String messageId) {
    int paddingSize = messageSize - messageId.length() - 1;
    String padding = getRandomPadding(paddingSize);
    return messageId + divChar + padding;
  }

  public String extractMessageId(String message) {
    int messageIdSize = message.lastIndexOf(divChar);
    if (messageIdSize < 0) {
      messageIdSize = message.length();
    }

    return message.substring(0, messageIdSize);
  }

  private void init(int preIndicesSize) {
    Random random = new Random();
    preGeneratedIndices = new int[preIndicesSize];
    curIdx = 0;

    for (int i = 0; i < preGeneratedIndices.length; i++) {
      preGeneratedIndices[i] = random.nextInt(paddingCharacters.length());
    }
  }

  private String getRandomPadding(int size) {
    StringBuilder paddedString = new StringBuilder();
    for (int i = 0; i < size; i++) {
      if (curIdx >= preGeneratedIndices.length) {
        curIdx = 0;
      }
      char randomChar = paddingCharacters.charAt(preGeneratedIndices[curIdx]);
      paddedString.append(randomChar);
      curIdx += 1;
    }

    return paddedString.toString();
  }
}
