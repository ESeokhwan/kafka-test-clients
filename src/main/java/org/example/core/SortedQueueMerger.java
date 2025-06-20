package org.example.core;

import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

public class SortedQueueMerger {
  public static <T extends Comparable<T>> Queue<T> mergeSortedQueues(
      List<Queue<T>> queues) {
    Queue<T> result = new LinkedList<>();
    PriorityQueue<QueueElement<T>> minHeap = new PriorityQueue<>();

    for (int i = 0; i < queues.size(); i++) {
      if (!queues.get(i).isEmpty()) {
        minHeap.add(new QueueElement<T>(queues.get(i).poll(), i));
      }
    }

    while (!minHeap.isEmpty()) {
      QueueElement<T> smallest = minHeap.poll();
      result.add(smallest.value);

      Queue<T> sourceQueue = queues.get(smallest.queueIndex);
      if (!sourceQueue.isEmpty()) {
        minHeap.add(new QueueElement<T>(sourceQueue.poll(), smallest.queueIndex));
      }
    }

    return result;
  }

  static class QueueElement<T extends Comparable<T>> implements Comparable<QueueElement<T>> {
    T value;
    int queueIndex;

    QueueElement(T value, int queueIndex) {
      this.value = value;
      this.queueIndex = queueIndex;
    }

    @Override
    public int compareTo(QueueElement<T> other) {
      if (other == null) return 1;
      if (this.value == null && other.value == null) return 0;
      if (this.value == null) return -1;
      if (other.value == null) return 1;
      return this.value.compareTo(other.value);
    }
  }
}

