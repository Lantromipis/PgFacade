package com.lantromipis.utils;

import java.util.List;

public class MathUtils {
    public static double calculateExpectedValue(List<Long> list) {
        double prb = 1.0 / list.size();
        return list.stream()
                .mapToLong(i -> i)
                .mapToDouble(i -> i * prb)
                .sum();
    }

    public static long calculateMaxValue(List<Long> list) {
        return list.stream()
                .mapToLong(i -> i)
                .max()
                .getAsLong();
    }

    public static long calculateMinValue(List<Long> list) {
        return list.stream()
                .mapToLong(i -> i)
                .min()
                .getAsLong();
    }
}
