package org.example.core;

import org.example.core.util.NoiseUtils;
import org.example.core.util.Noises;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractService implements IService {

    private final int roundCnt;
    private final int interval;
    private final Noises noises;
    private final AtomicBoolean closeScheduled = new AtomicBoolean(false);

    private final AtomicInteger curIdxReserved = new AtomicInteger(0);

    protected AbstractService(int roundCnt, int interval, Noises noises) {
        this.roundCnt = roundCnt;
        this.interval = interval;
        this.noises = noises;
    }

    protected AbstractService(int roundCnt, int interval, double intervalNoiseStddev, int intervalMaxAbsNoise, Random randomEngine) {
        this.roundCnt = roundCnt;
        this.interval = interval;
        this.noises = NoiseUtils.generateNoises(
                intervalNoiseStddev, intervalMaxAbsNoise,
                Math.min(roundCnt, NoiseUtils.MAX_NOISE_LIST_LENGTH), randomEngine
        );
    }

    @Override
    public int curInterval() {
        int curNoise = noises.next();
        if (curIdxReserved.get() <= 0) return Math.abs(curNoise);
        return interval + curNoise;
    }

    @Override
    public boolean hasMore() {
        return curIdxReserved.get() < roundCnt;
    }

    @Override
    public boolean reserve() {
        int afterReserve = curIdxReserved.incrementAndGet();
        return afterReserve < roundCnt;
    }

    @Override
    public boolean closeScheduled() {
        return closeScheduled.getAndSet(true);
    }
}
