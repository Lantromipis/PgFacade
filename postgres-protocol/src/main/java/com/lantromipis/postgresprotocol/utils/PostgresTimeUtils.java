package com.lantromipis.postgresprotocol.utils;

import java.util.concurrent.TimeUnit;

public class PostgresTimeUtils {
    private static final long POSTGRES_EPOCH_2000_01_01 = 946684800000L;
    private static final long NANOS_PER_MILLISECOND = 1000000L;

    public static long getNowInPostgresTime() {
        long now = System.nanoTime() / NANOS_PER_MILLISECOND;
        return TimeUnit.MICROSECONDS.convert(
                (now - POSTGRES_EPOCH_2000_01_01),
                TimeUnit.MICROSECONDS
        );
    }
}
