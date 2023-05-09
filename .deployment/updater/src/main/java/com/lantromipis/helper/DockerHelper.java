package com.lantromipis.helper;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.lantromipis.properties.UpdaterProperties;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;

@Slf4j
@ApplicationScoped
public class DockerHelper {

    @Inject
    UpdaterProperties updaterProperties;

    private DockerClient dockerClient;

    private static final String DOCKER_ENV_VAR_HOSTNAME = "HOSTNAME";
    private static final String POSTGRES_ENV_VAR_USERNAME = "POSTGRES_USER";
    private static final String POSTGRES_ENV_VAR_PASSWORD = "POSTGRES_PASSWORD";
    private static final String POSTGRES_ENV_VAR_DB = "POSTGRES_DB";

    private static final String TEMP_NETWORK_NAME = "7bdf8838-5593-408b-bf17-142934513822";

    private void initIfNeeded() {
        if (dockerClient != null) {
            return;
        }

        DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost(updaterProperties.docker().host())
                .build();

        DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost(config.getDockerHost())
                .sslConfig(config.getSSLConfig())
                .build();

        dockerClient = DockerClientImpl.getInstance(config, httpClient);
    }

    public String createAndStartNewPostgres(String imageTag, String superName, String superPass, String superDb) throws InterruptedException {
        initIfNeeded();
        CreateContainerResponse createContainerResponse = dockerClient.createContainerCmd(imageTag)
                .withHostConfig(
                        HostConfig.newHostConfig()
                )
                .withEnv(
                        List.of(
                                createEnvValueForRequest(POSTGRES_ENV_VAR_PASSWORD, superName),
                                createEnvValueForRequest(POSTGRES_ENV_VAR_USERNAME, superPass),
                                createEnvValueForRequest(POSTGRES_ENV_VAR_DB, superDb)
                        )
                )
                .exec();

        String containerId = createContainerResponse.getId();
        dockerClient.startContainerCmd(containerId).exec();

        Thread.sleep(15000);

        return containerId;
    }

    public String connectPostgresAndCurrentContainerTogether(String postgresContainerId) {
        initIfNeeded();

        dockerClient
                .createNetworkCmd()
                .withName(TEMP_NETWORK_NAME)
                .exec();

        String hostname = System.getenv(DOCKER_ENV_VAR_HOSTNAME);

        if (hostname == null) {
            throw new RuntimeException("Docker error. No HOSTNAME env var in container.");
        }

        InspectContainerResponse inspectSelfResponse = dockerClient
                .inspectContainerCmd(hostname)
                .exec();

        dockerClient.connectToNetworkCmd()
                .withContainerId(inspectSelfResponse.getId())
                .withNetworkId(TEMP_NETWORK_NAME)
                .exec();

        dockerClient.connectToNetworkCmd()
                .withContainerId(postgresContainerId)
                .withNetworkId(TEMP_NETWORK_NAME)
                .exec();

        InspectContainerResponse inspectPostgresResponse = dockerClient
                .inspectContainerCmd(postgresContainerId)
                .exec();

        return inspectPostgresResponse.getNetworkSettings()
                .getNetworks()
                .get(TEMP_NETWORK_NAME)
                .getIpAddress();
    }

    public void cleanup(String postgresContainerId) {
        try {
            String hostname = System.getenv(DOCKER_ENV_VAR_HOSTNAME);

            if (hostname == null) {
                throw new RuntimeException("Docker error. No HOSTNAME env var in container.");
            }

            InspectContainerResponse inspectSelfResponse = dockerClient
                    .inspectContainerCmd(hostname)
                    .exec();

            try {
                dockerClient.disconnectFromNetworkCmd()
                        .withNetworkId(TEMP_NETWORK_NAME)
                        .withContainerId(inspectSelfResponse.getId())
                        .exec();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }

            try {
                if (postgresContainerId != null) {
                    dockerClient.disconnectFromNetworkCmd()
                            .withNetworkId(TEMP_NETWORK_NAME)
                            .withContainerId(postgresContainerId)
                            .exec();
                } else {
                    log.warn("Postgres container id is null. Network not removed");
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }

            dockerClient.removeNetworkCmd(TEMP_NETWORK_NAME).exec();
        } catch (Exception e) {
            log.error("Failed to remove network", e);
        }
    }

    private String createEnvValueForRequest(String varName, String value) {
        return varName + "=" + value;
    }
}
