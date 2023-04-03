package com.lantromipis;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "tests.performance")
public interface PerformanceTestsProperties {

    boolean runPgBouncer();

    HostProperties pgFacadeHost();

    HostProperties originalPostgresHost();

    HostProperties pgBouncerHost();

    UserProperties pgUser();

    AcquireConnectionTimeProperties testAquireConnectionTime();

    DelayTestProperties delayTest();

    LoadTestProperties loadTest();

    interface HostProperties {
        String host();

        int port();
    }

    interface UserProperties {
        String database();

        String user();

        String password();
    }

    interface AcquireConnectionTimeProperties {
        int retries();

        long waitBetween();
    }

    interface DelayTestProperties {
        int tableSizeKb();

        int retries();
    }

    interface LoadTestProperties {
        int tableSizeKb();

        int retries();

        int firstNumOfConnections();

        int secondNumOfConnections();

        int thirdNumOfConnections();
    }
}
