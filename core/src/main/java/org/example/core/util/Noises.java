package org.example.core.util;

import lombok.Getter;

import java.util.List;

public class Noises {

    private final List<Integer> noises;

    @Getter
    private int curIdx = 0;

    public Noises(List<Integer> noises) {
        this.noises = noises;
    }

    public int next() {
        int prevNoise = 0;
        if (!noises.isEmpty()) prevNoise = noises.get(curIdx % noises.size());
        curIdx += 1;
        return prevNoise;
    }

    public static Noises copy(Noises noises) {
        return new Noises(noises.noises);
    }
}
