package org.example.util;

public class TimeUtils {

    public static long getCurrentTimeMillis() {
        return System.currentTimeMillis();
    }

    public static long getCurrentTimeNanos() {
        return System.nanoTime();
    }

    public static long getAccurateCurrentTimeMillis() {
        return System.nanoTime() / 1_000_000;
    }
}
