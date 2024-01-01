package com.lantromipis.configuration.utils;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;

public class PostgresSettingsUtils {

    @Getter
    @RequiredArgsConstructor
    public enum PgMemoryUnit {
        BYTES("B", 1),
        KILOBYTES("kB", 1024),
        MEGABYTES("MB", 1048576),
        GIGABYTES("GB", 1073741824),
        TERABYTES("TB", 1099511627776L);

        private final String textValue;
        private final long sizeInBytes;

        public static PgMemoryUnit from(String unitTextValue) throws IllegalArgumentException {
            return Arrays.stream(PgMemoryUnit.values())
                    .filter(v -> v.textValue.equals(unitTextValue))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unknown memory unit '" + unitTextValue + "'"));
        }
    }

    public static long convertPgMemoryValueToBytes(int value, String unit) {
        PgMemoryUnit originalUnit = PgMemoryUnit.from(unit);

        return value * originalUnit.sizeInBytes;
    }
}
