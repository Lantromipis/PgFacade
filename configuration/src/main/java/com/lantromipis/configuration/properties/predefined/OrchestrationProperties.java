package com.lantromipis.configuration.properties.predefined;

import io.smallrye.config.ConfigMapping;

import java.time.Duration;

@ConfigMapping(prefix = "pg-facade.orchestration")
public interface OrchestrationProperties {

    AdapterType adapter();

    NoAdapterProperties noAdapter();

    DockerProperties docker();

    CommonProperties common();

    interface CommonProperties {

        PostgresStartupCheckProperties postgresStartupCheck();

        PostgresDeadCheckProperties postgresDeadCheck();

        StandbyProperties standby();

        interface StandbyProperties {
            int count();

            Duration countCheckInterval();
        }

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

    interface NoAdapterProperties {
        String primaryHost();

        int primaryPort();
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
        /**
         * Use when there is no need in orchestration. If active, PgFacade will work like proxy + connection pool without any HA features.
         */
        NO_ADAPTER,

        /**
         * If enabled, PgFacade will work in Docker.
         */
        DOCKER
    }
}
