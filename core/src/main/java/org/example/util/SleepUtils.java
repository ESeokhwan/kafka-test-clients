package org.example.util;

public class SleepUtils {

    public static void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void sleepNanos(long nanos) {
        long millis = nanos / 1_000_000;
        int extraNanos = (int) (nanos % 1_000_000);
        try {
            Thread.sleep(millis, extraNanos);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
