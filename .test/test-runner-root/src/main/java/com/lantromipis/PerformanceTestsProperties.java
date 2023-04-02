package com.lantromipis;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "tests.performance")
public interface PerformanceTestsProperties {

    PostgresConfProperties postgresConf();

    AcquireConnectionTimeProperties testAquireConnectionTime();

    interface PostgresConfProperties {
        String host();

        int port();

        String database();

        String user();

        String password();
    }

    interface AcquireConnectionTimeProperties {
        int retries();

        long waitBetween();
    }
}
