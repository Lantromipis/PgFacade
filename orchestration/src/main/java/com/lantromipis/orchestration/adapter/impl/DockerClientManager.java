package com.lantromipis.orchestration.adapter.impl;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.orchestration.exception.InitializationException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ApplicationScoped
public class DockerClientManager {

    @Inject
    OrchestrationProperties orchestrationProperties;

    private DockerClient dockerClient;

    public void init() {
        log.info("Initializing Docker client");

        try {
            OrchestrationProperties.DockerProperties dockerProperties = orchestrationProperties.docker();

            DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                    .withDockerHost(dockerProperties.host())
/*                .withDockerTlsVerify(true)
                .withDockerCertPath("/home/user/.docker")
                .withRegistryUsername(registryUser)
                .withRegistryPassword(registryPass)
                .withRegistryEmail(registryMail)
                .withRegistryUrl(registryUrl)*/
                    .build();

            DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                    .dockerHost(config.getDockerHost())
                    .sslConfig(config.getSSLConfig())
                    .build();

            dockerClient = DockerClientImpl.getInstance(config, httpClient);

            // validate Postgres network exist
            try {
                dockerClient.inspectNetworkCmd()
                        .withNetworkId(orchestrationProperties.docker().postgres().networkName())
                        .exec();
            } catch (NotFoundException e) {
                throw new InitializationException("Docker network for Postgres not found. Expected network name: '" + orchestrationProperties.docker().postgres().networkName() + "'. Create this network or/and change PgFacade configuration.");
            }

            // validate PgFacade internal network exist
            try {
                dockerClient.inspectNetworkCmd()
                        .withNetworkId(orchestrationProperties.docker().pgFacade().internalNetworkName())
                        .exec();
            } catch (NotFoundException e) {
                throw new InitializationException("Docker network for PgFacade internal needs not found. Expected network name: '" + orchestrationProperties.docker().pgFacade().internalNetworkName() + "'. Create this network or/and change PgFacade configuration.");
            }

            // validate PgFacade external network exist
            try {
                dockerClient.inspectNetworkCmd()
                        .withNetworkId(orchestrationProperties.docker().pgFacade().externalNetworkName())
                        .exec();
            } catch (NotFoundException e) {
                throw new InitializationException("Docker networks for PgFacade external usage not found. Expected network name: '" + orchestrationProperties.docker().pgFacade().externalNetworkName() + "'. Create this network or/and change PgFacade configuration.");
            }
        } catch (InitializationException e) {
            throw e;
        } catch (Exception e) {
            throw new InitializationException("Failed to initialize Docker platform adapter! Unexpected error ", e);
        }

        log.info("Successfully initialized Docker client");
    }

    public void shutdown() {
        log.info("Shutting down Docker client");
        try {
            dockerClient.close();
            dockerClient = null;
        } catch (Exception ignored) {
        }
        log.info("Successfully shut down Docker client");
    }

    public DockerClient getDockerClient() {
        if (dockerClient == null) {
            throw new InitializationException("Docker client not initialized");
        }

        return dockerClient;
    }
}
