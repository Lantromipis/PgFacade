package com.lantromipis.configuration.properties.predefined;

import io.smallrye.config.ConfigMapping;

import java.time.Duration;

@ConfigMapping(prefix = "pg-facade.orchestration")
public interface OrchestrationProperties {

    AdapterType adapter();

    DockerProperties docker();

    CommonProperties common();

    interface CommonProperties {

        PostgresStartupCheckProperties postgresStartupCheck();

        PostgresDeadCheckProperties postgresDeadCheck();

        int standByCount();

        interface PostgresStartupCheckProperties {
            long startPeriod();

            long interval();

            long retries();
        }

        interface PostgresDeadCheckProperties {
            Duration interval();

            int retries();
        }
    }

    interface DockerProperties {
        String dockerHost();

        String postgresNetworkName();

        String postgresContainerName();

        String postgresVolumeName();

        String postgresImageTag();

        String postgresImagePgDataDir();

        String helperObjectName();

        HealthCheckProperties postgresHealthcheck();

        interface HealthCheckProperties {
            long interval();

            long timeout();

            int retries();

            long startPeriod();

            String cmdShellCommand();
        }

    }

    enum AdapterType {
        DOCKER
    }
}
