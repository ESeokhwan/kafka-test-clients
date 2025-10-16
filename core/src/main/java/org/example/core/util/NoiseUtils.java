package org.example.core.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class NoiseUtils {

    public static int SMALL_NOISE_LIST_LENGTH = 1_000;

    public static int MAX_NOISE_LIST_LENGTH = 1_000_000;

    public static List<Integer> generateNoiseList(double stddev, int maxAbsNoise, int length, Random randomEngine) {
        List<Integer> noises = new ArrayList<>(length);
        if (maxAbsNoise < 0) maxAbsNoise = -maxAbsNoise;
        for (int i = 0; i < length; i++) {
            double generatedValue = randomEngine.nextGaussian() * stddev;
            int noise = (int) Math.round(generatedValue);
            noises.add(Math.max(-maxAbsNoise, Math.min(noise, maxAbsNoise)));
        }
        return noises;
    }

}
