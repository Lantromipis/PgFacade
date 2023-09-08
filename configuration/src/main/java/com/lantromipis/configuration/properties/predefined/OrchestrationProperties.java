package com.lantromipis.configuration.properties.predefined;

import io.smallrye.config.ConfigMapping;

import java.math.BigDecimal;
import java.time.Duration;

@ConfigMapping(prefix = "pg-facade.orchestration")
public interface OrchestrationProperties {

    AdapterType adapter();

    PostgresClusterRestoreProperties postgresClusterRestore();

    NoAdapterProperties noAdapter();

    DockerProperties docker();

    CommonProperties common();

    interface PostgresClusterRestoreProperties {
        boolean autoRestoreLostCluster();

        boolean removeFailedToRestoreInstance();
    }

    interface CommonProperties {

        PostgresStartupCheckProperties postgresStartupCheck();

        PostgresDeadCheckProperties postgresDeadCheck();

        StandbyProperties standby();

        ExternalLoadBalancerProperties externalLoadBalancer();

        interface ExternalLoadBalancerProperties {
            boolean deploy();

            Duration healthcheckInterval();

            Duration healthcheckAwait();
        }

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

        ExternalLoadBalancerProperties externalLoadBalancer();

        interface ExternalLoadBalancerProperties {
            String networkForEndClients();

            String dnsAlias();

            String containerName();

            String imageTag();

            DockerContainerResources resources();
        }

        interface PgFacadeProperties {

            String internalNetworkName();

            String externalNetworkName();

            String containerName();

            String expectedDockerSockFileName();

            DockerContainerResources resources();
        }

        interface PostgresProperties {
            String imageTag();

            String imagePgData();

            String networkName();

            String containerName();

            String volumeName();

            DockerContainerResources resources();
        }

        interface DockerContainerResources {
            BigDecimal cpuLimit();

            String memoryLimit();
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
