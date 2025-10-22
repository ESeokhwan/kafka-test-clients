package org.example.core;

import java.io.Closeable;

public interface IService extends Closeable {

    int curInterval();

    boolean hasMore();

    boolean isDone();

    boolean reserve();

    void work();

    boolean closeScheduled();
}
