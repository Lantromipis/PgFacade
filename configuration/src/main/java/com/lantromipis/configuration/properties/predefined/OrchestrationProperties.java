package com.lantromipis.configuration.properties.predefined;

import io.smallrye.config.ConfigMapping;

import java.time.Duration;

@ConfigMapping(prefix = "pg-facade.orchestration")
public interface OrchestrationProperties {

    AdapterType adapter();

    PostgresClusterRestoreProperties postgresClusterRestore();

    NoAdapterProperties noAdapter();

    DockerProperties docker();

    CommonProperties common();

    interface PostgresClusterRestoreProperties {
        boolean autoRestoreIfNoInstancesOnStartup();

        boolean allowCreatingNewEmptyPrimaryIfRestoreOnStartupFailed();

        boolean autoRestoreLostCluster();

        boolean removeFailedToRestoreInstance();
    }

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
        String host();

        PgFacadeProperties pgFacade();

        PostgresProperties postgres();

        String helperObjectName();

        interface PgFacadeProperties {
            String localFilesDirectory();

            String networkName();

            String containerName();

            String expectedDockerSockFileName();
        }

        interface PostgresProperties {
            String imageTag();

            String imagePgData();

            String networkName();

            String containerName();

            String volumeName();

            HealthCheckProperties healthcheck();

            interface HealthCheckProperties {
                long interval();

                long timeout();

                int retries();

                long startPeriod();

                String cmdShellCommand();
            }
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
