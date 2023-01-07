package com.lantromipis.configuration.predefined;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "pg-facade.orchestration")
public interface OrchestrationProperties {

    AdapterType adapter();

    DockerProperties docker();

    CommonProperties common();

    interface CommonProperties {

        PostgresStartupCheckProperties postgresStartupCheck();

        PostgresDeadCheckProperties postgresDeadCheck();

        interface PostgresStartupCheckProperties {
            long startPeriod();

            long interval();

            long retries();
        }

        interface PostgresDeadCheckProperties {
            long interval();

            int retries();
        }
    }

    interface DockerProperties {
        String dockerHost();

        String postgresNetworkName();

        String postgresContainerName();

        String postgresImageTag();

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
