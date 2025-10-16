package org.example.core;

import java.io.Closeable;

public interface IService extends Closeable {

    int curInterval();

    boolean hasMore();

    void work();
}
