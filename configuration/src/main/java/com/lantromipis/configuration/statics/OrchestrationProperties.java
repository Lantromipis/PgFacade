package com.lantromipis.configuration.statics;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "pg-facade.orchestration")
public interface OrchestrationProperties {

    AdapterType adapter();

    DockerProperties docker();

    interface DockerProperties {
        String dockerHost();

        String postgresNetworkName();

        String postgresContainerName();

        String postgresImageTag();
    }

    enum AdapterType {
        DOCKER
    }
}
