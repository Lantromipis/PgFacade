package com.lantromipis.orchestration.adapter.impl;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.HealthState;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.HealthCheck;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.lantromipis.configuration.statics.OrchestrationProperties;
import com.lantromipis.configuration.statics.PostgresProperties;
import com.lantromipis.orchestration.adapter.api.OrchestrationAdapter;
import com.lantromipis.orchestration.constant.DockerConstants;
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

        //to pre-load all containers
        getAvailablePostgresInstancesInfos();

        log.info("Successfully created Docker client for cluster management.");
    }

    @Override
    public UUID createNewPostgresInstance(PostgresInstanceCreationRequest request) {
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
                                    .withInterval(TimeUnit.MILLISECONDS.toNanos(dockerProperties.postgresHealthcheck().interval()))
                                    .withRetries(dockerProperties.postgresHealthcheck().retries())
                                    .withStartPeriod(TimeUnit.MILLISECONDS.toNanos(dockerProperties.postgresHealthcheck().startPeriod()))
                                    .withTimeout(TimeUnit.MILLISECONDS.toNanos(dockerProperties.postgresHealthcheck().timeout()))
                                    .withTest(List.of(DockerConstants.HEALTHCHECK_CMD_SHELL, dockerProperties.postgresHealthcheck().cmdShellCommand()))
                    )
                    .exec();

            return rememberContainer(createResponse.getId());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public boolean startPostgresInstance(UUID instanceId) {
        String containerId = instanceIdAndContainerIdMap.get(instanceId);
        if (containerId == null) {
            log.error("Error starting Docker postgres container. Instance not found.");
            return false;
        }

        try {
            dockerClient.startContainerCmd(containerId).exec();
            return true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    @Override
    public List<PostgresInstanceInfo> getAvailablePostgresInstancesInfos() {
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
                    .instanceAddress(dockerUtils.getContainerAddress(container))
                    .instancePort(5432) //TODO maybe need to change
                    .status(dockerMapper.toInstanceStatus(container.getState()))
                    .master(true) //TODO constant until hot-standby!!!
                    .build()
            );
        }

        return ret;
    }

    @Override
    public PostgresInstanceInfo getInstanceInfo(UUID instanceId) {
        String containerId = instanceIdAndContainerIdMap.get(instanceId);

        if (containerId == null) {
            return null;
        }

        try {
            InspectContainerResponse inspectResponse = dockerClient.inspectContainerCmd(containerId).exec();

            String healthState = Optional.of(inspectResponse.getState())
                    .map(InspectContainerResponse.ContainerState::getHealth)
                    .map(HealthState::getStatus)
                    .orElse(null);

            return PostgresInstanceInfo
                    .builder()
                    .instanceId(instanceId)
                    .instanceAddress(dockerUtils.getContainerAddress(inspectResponse))
                    .instancePort(5432) //TODO maybe need to change
                    .status(dockerMapper.toInstanceStatus(inspectResponse.getState().getStatus()))
                    .health(dockerMapper.toInstanceHealth(healthState))
                    .master(true) //TODO constant until hot-standby!!!
                    .build();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
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
}
