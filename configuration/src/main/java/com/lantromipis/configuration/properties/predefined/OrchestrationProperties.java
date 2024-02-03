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
        boolean removeFailedToRestoreInstance();
    }

    interface CommonProperties {

        ExternalLoadBalancerProperties externalLoadBalancer();

        PostgresCommonProperties postgres();

        interface ExternalLoadBalancerProperties {
            boolean deploy();

            Duration healthcheckInterval();

            Duration startupDuration();

            Duration healthcheckTimeout();

            int healthcheckRetries();
        }

        interface PostgresCommonProperties {

            CommonPostgresProperties common();

            PrimaryPostgresProperties primary();

            StandbyPostgresProperties standby();

            interface CommonPostgresProperties {

                CommonPostgresReadinessProperties readiness();

                interface CommonPostgresReadinessProperties {
                    Duration delay();

                    Duration interval();

                    int retries();
                }
            }

            interface PrimaryPostgresProperties {

                PrimaryHealthcheckPostgresProperties healthcheck();

                interface PrimaryHealthcheckPostgresProperties {
                    Duration interval();

                    int retries();

                    Duration timeout();
                }
            }

            interface StandbyPostgresProperties {
                int count();

                StandbyHealthcheckPostgresProperties healthcheck();

                interface StandbyHealthcheckPostgresProperties {
                    Duration interval();

                    Duration timeout();
                }
            }
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
