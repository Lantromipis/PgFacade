package com.lantromipis.orchestration.adapter.impl;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.HealthState;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.HealthCheck;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Network;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.lantromipis.configuration.statics.OrchestrationProperties;
import com.lantromipis.configuration.statics.PostgresProperties;
import com.lantromipis.orchestration.adapter.api.OrchestrationAdapter;
import com.lantromipis.orchestration.constant.DockerConstants;
import com.lantromipis.orchestration.exception.AdapterInitializationException;
import com.lantromipis.orchestration.exception.PostgresInstanceCreationException;
import com.lantromipis.orchestration.exception.PostgresInstanceStartException;
import com.lantromipis.orchestration.mapper.DockerMapper;
import com.lantromipis.orchestration.model.PostgresInstanceCreationRequest;
import com.lantromipis.orchestration.model.PostgresInstanceInfo;
import com.lantromipis.orchestration.util.DockerUtils;
import io.quarkus.arc.lookup.LookupIfProperty;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@ApplicationScoped
@LookupIfProperty(name = "pg-facade.pg-cluster-management.adapter", stringValue = "docker")
public class DockerBasedOrchestrationAdapter implements OrchestrationAdapter {

    @Inject
    OrchestrationProperties orchestrationProperties;

    @Inject
    DockerMapper dockerMapper;

    @Inject
    PostgresProperties postgresProperties;

    @Inject
    DockerUtils dockerUtils;

    private DockerClient dockerClient;
    private String postgresNetwork;
    private ConcurrentHashMap<UUID, String> instanceIdAndContainerIdMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, UUID> containerIdAndInstanceIdMap = new ConcurrentHashMap<>();

    public void initialize() {
        log.info("Docker is selected as orchestrator adapter.");
        OrchestrationProperties.DockerProperties dockerProperties = orchestrationProperties.docker();

        DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost(dockerProperties.dockerHost())
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

        postgresNetwork = getNetworkIdByName(dockerProperties.postgresNetworkName());

        //to pre-load all containers
        getAvailablePostgresInstances();

        log.info("Successfully created Docker client for cluster management.");
    }

    @Override
    public PostgresInstanceInfo createNewPostgresInstance(PostgresInstanceCreationRequest request) throws PostgresInstanceCreationException {
        try {
            OrchestrationProperties.DockerProperties dockerProperties = orchestrationProperties.docker();

            var createResponse = dockerClient.createContainerCmd(dockerProperties.postgresImageTag())
                    .withName(dockerProperties.postgresContainerName() + "-" + UUID.randomUUID())
                    .withHostConfig(
                            HostConfig.newHostConfig()
                                    .withNetworkMode(dockerProperties.postgresNetworkName())
                    )
                    .withEnv(
                            List.of(createEnvValueForRequest(DockerConstants.POSTGRES_ENV_VAR_PASSWORD, postgresProperties.pgFacadePassword()),
                                    createEnvValueForRequest(DockerConstants.POSTGRES_ENV_VAR_USERNAME, postgresProperties.pgFacadeUser()),
                                    createEnvValueForRequest(DockerConstants.POSTGRES_ENV_VAR_DB, postgresProperties.pgFacadeDatabase())
                            )
                    )
                    .withHealthcheck(
                            new HealthCheck()
                                    .withInterval(postgresProperties.healthcheck().interval())
                                    .withRetries(postgresProperties.healthcheck().retries())
                                    .withStartPeriod(postgresProperties.healthcheck().startPeriod())
                                    .withTimeout(postgresProperties.healthcheck().timeout())
                                    .withTest(List.of(DockerConstants.HEALTHCHECK_CMD_SHELL, postgresProperties.healthcheck().command()))
                    )
                    .exec();

            String containerId = createResponse.getId();

            dockerClient.startContainerCmd(containerId).exec();

            var inspectResponse = dockerClient.inspectContainerCmd(containerId).exec();

            String containerIpAddress = dockerUtils.getContainerIpAddress(inspectResponse);

            if (containerIpAddress == null) {
                throw new PostgresInstanceCreationException("Unable to obtain IP address of Docker container.");
            }

            if (!waitBlockingUntilContainerHealthy(containerId)) {
                throw new PostgresInstanceCreationException("Health check timeout.");
            }

            UUID instanceId = rememberContainer(containerId);

            return PostgresInstanceInfo
                    .builder()
                    .instanceIpAddress(containerIpAddress)
                    .instanceId(instanceId)
                    .status(dockerMapper.from(inspectResponse.getState().getStatus()))
                    .master(request.isMaster())
                    .build();

        } catch (PostgresInstanceCreationException postgresInstanceCreationException) {
            throw postgresInstanceCreationException;
        } catch (Exception e) {
            throw new PostgresInstanceCreationException("Unable to create Docker container.", e);
        }
    }

    @Override
    public PostgresInstanceInfo tryToStartPostgresInstance(UUID instanceId) throws PostgresInstanceStartException {
        String containerId = instanceIdAndContainerIdMap.get(instanceId);
        if (containerId == null) {
            throw new PostgresInstanceStartException("Error starting Docker postgres container.");
        }
        try {
            dockerClient.startContainerCmd(containerId).exec();
            var inspectResponse = dockerClient.inspectContainerCmd(containerId).exec();

            String containerIpAddress = dockerUtils.getContainerIpAddress(inspectResponse);

            if (!waitBlockingUntilContainerHealthy(containerId)) {
                throw new PostgresInstanceStartException("Health check timeout.");
            }

            return PostgresInstanceInfo
                    .builder()
                    .master(true)//TODO until hot-standby
                    .instanceIpAddress(containerIpAddress)
                    .instanceId(instanceId)
                    .status(dockerMapper.from(inspectResponse.getState().getStatus()))
                    .build();
        } catch (PostgresInstanceStartException postgresInstanceStartException) {
            throw postgresInstanceStartException;
        } catch (Exception e) {
            throw new PostgresInstanceStartException("Unable to start Docker postgres container with id " + containerId, e);
        }
    }

    @Override
    public List<PostgresInstanceInfo> getAvailablePostgresInstances() {
        OrchestrationProperties.DockerProperties dockerProperties = orchestrationProperties.docker();

        List<Container> containers = dockerClient.listContainersCmd()
                .withShowAll(true)
                .withNameFilter(List.of(dockerProperties.postgresContainerName()))
                .exec();

        if (CollectionUtils.isEmpty(containers)) {
            return Collections.emptyList();
        }

        List<PostgresInstanceInfo> ret = new ArrayList<>();

        for (Container container : containers) {
            UUID instanceId = containerIdAndInstanceIdMap.get(container.getId());

            if (instanceId == null) {
                instanceId = rememberContainer(container.getId());
            }

            ret.add(PostgresInstanceInfo
                    .builder()
                    .instanceId(instanceId)
                    .instanceIpAddress(dockerUtils.getContainerIpAddress(container))
                    .status(dockerMapper.from(container.getState()))
                    .master(true) //TODO constant until hot-standby!!!
                    .build()
            );
        }

        return ret;
    }

    private String getNetworkIdByName(String networkName) {
        String networkId;

        List<Network> pgFacadePossibleNetworks = dockerClient.listNetworksCmd().withNameFilter(networkName).exec();

        if (CollectionUtils.isEmpty(pgFacadePossibleNetworks)) {
            //var createNetworkResponse = dockerClient.createNetworkCmd().withName(dockerProperties.postgresNetworkName()).withCheckDuplicate(true).exec();
            networkId = null;
        } else {
            networkId = pgFacadePossibleNetworks
                    .stream()
                    .filter(network -> Objects.equals(network.getName(), networkName))
                    .findFirst()
                    .map(Network::getId)
                    .orElse(null);
        }

        if (networkId == null) {
            throw new AdapterInitializationException("Unable to find Docker network for PgFacade with name '" + networkName + "'. PgFacade will not work!");
        }

        return networkId;
    }

    private UUID rememberContainer(String containerId) {
        UUID instanceId = UUID.randomUUID();

        instanceIdAndContainerIdMap.put(instanceId, containerId);
        containerIdAndInstanceIdMap.put(containerId, instanceId);

        return instanceId;
    }

    private String createEnvValueForRequest(String varName, String value) {
        return varName + "=" + value;
    }

    private boolean waitBlockingUntilContainerHealthy(String containerId) {
        long timeoutMilliseconds = TimeUnit.MILLISECONDS.convert(
                postgresProperties.healthcheck().timeout()
                        * postgresProperties.healthcheck().retries()
                        + postgresProperties.healthcheck().startPeriod(),
                TimeUnit.NANOSECONDS
        );

        long pollingMilliseconds = TimeUnit.MILLISECONDS.convert(
                postgresProperties.healthcheck().timeout(),
                TimeUnit.NANOSECONDS
        );

        try {
            long startTime = System.currentTimeMillis();
            while (true) {
                var inspectResponse = dockerClient.inspectContainerCmd(containerId).exec();

                String healthStatus = Optional.of(inspectResponse)
                        .map(InspectContainerResponse::getState)
                        .map(InspectContainerResponse.ContainerState::getHealth)
                        .map(HealthState::getStatus)
                        .orElse(null);

                log.info(healthStatus);

                if (DockerConstants.ContainerHealth.HEALTHY.getValue().equals(healthStatus)) {
                    return true;
                } else {
                    log.info(String.valueOf((startTime + timeoutMilliseconds)) + " " + System.currentTimeMillis());
                    if (startTime + timeoutMilliseconds < System.currentTimeMillis()) {
                        return false;
                    }
                    Thread.sleep(pollingMilliseconds);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error(e.getMessage(), e);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return false;
    }
}
