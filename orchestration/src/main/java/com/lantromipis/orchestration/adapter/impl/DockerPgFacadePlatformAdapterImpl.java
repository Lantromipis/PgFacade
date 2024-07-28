package com.lantromipis.orchestration.adapter.impl;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Volume;
import com.lantromipis.configuration.properties.constant.PgFacadeConstants;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.predefined.ProxyProperties;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.PgFacadePlatformAdapter;
import com.lantromipis.orchestration.constant.DockerConstants;
import com.lantromipis.orchestration.exception.PlatformAdapterOperationExecutionException;
import com.lantromipis.orchestration.model.PgFacadeNodeExternalConnectionsInfo;
import com.lantromipis.orchestration.model.PgFacadeNodeHttpConnectionsInfo;
import com.lantromipis.orchestration.model.PgFacadeRaftNodeInfo;
import com.lantromipis.orchestration.util.DockerUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@ApplicationScoped
public class DockerPgFacadePlatformAdapterImpl implements PgFacadePlatformAdapter {

    @Inject
    OrchestrationProperties orchestrationProperties;

    @Inject
    DockerUtils dockerUtils;

    @Inject
    DockerClientManager dockerClientManager;

    @Inject
    ProxyProperties proxyProperties;

    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    @Override
    public PgFacadeRaftNodeInfo getSelfRaftNodeInfo() throws PlatformAdapterOperationExecutionException {
        String hostname = System.getenv(DockerConstants.DOCKER_ENV_VAR_HOSTNAME);

        if (hostname == null) {
            throw new PlatformAdapterOperationExecutionException("Docker error. PgFacade container has no HOSTNAME env var. Container with PgFacade must have it, and this env var must contain Docker container ID start symbols.");
        }

        InspectContainerResponse inspectContainerResponse;

        try {
            inspectContainerResponse = dockerClientManager.getDockerClient()
                    .inspectContainerCmd(hostname)
                    .exec();
        } catch (Exception e) {
            throw new PlatformAdapterOperationExecutionException("Failed to insect self container.", e);
        }

        return Optional.ofNullable(inspectContainerResponseToPgFacadeRaftNodeInfo(inspectContainerResponse))
                .orElseThrow(() -> new PlatformAdapterOperationExecutionException("Can not define self IP address. Does container have HOSTNAME env var configured properly AND is connected to PgFacade network? This env var must contain short Docker container ID"));
    }

    @Override
    public List<PgFacadeRaftNodeInfo> getActiveRaftNodeInfos() throws PlatformAdapterOperationExecutionException {
        try {
            List<Container> containers = listPgFacadeUnsuspendedContainers();

            if (CollectionUtils.isEmpty(containers)) {
                return Collections.emptyList();
            }

            return containers
                    .stream()
                    .map(this::containerToPgFacadeRaftNodeInfo)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

        } catch (Exception e) {
            throw new PlatformAdapterOperationExecutionException("Failed to get Raft nodes info. ", e);
        }
    }

    @Override
    public PgFacadeRaftNodeInfo createAndStartNewPgFacadeInstance() throws PlatformAdapterOperationExecutionException {
        PgFacadeRaftNodeInfo self = getSelfRaftNodeInfo();
        InspectContainerResponse inspectSelfResponse = dockerClientManager.getDockerClient().inspectContainerCmd(self.getPlatformAdapterIdentifier()).exec();

        UUID instanceId = UUID.randomUUID();

        CreateContainerCmd createContainerCmd = dockerClientManager.getDockerClient().createContainerCmd(inspectSelfResponse.getImageId())
                .withName(dockerUtils.createUniqueObjectName(orchestrationProperties.docker().pgFacade().containerName(), instanceId.toString()))
                .withLabels(Map.of(PgFacadeConstants.DOCKER_SPECIFIC_PGFACADE_CONTAINER_LABEL, "true"));

        HostConfig newHostConfig = HostConfig.newHostConfig();

        if (inspectSelfResponse.getHostConfig().getBinds() != null) {
            for (var bind : inspectSelfResponse.getHostConfig().getBinds()) {
                if (bind.getVolume().getPath().contains(orchestrationProperties.docker().pgFacade().expectedDockerSockFileName())) {
                    newHostConfig
                            .withBinds(
                                    new Bind(
                                            bind.getPath(),
                                            new Volume(bind.getVolume().getPath())
                                    )
                            );
                    break;
                }
            }
        }

        // propagate env vars == propagate settings
        if (inspectSelfResponse.getConfig().getEnv() != null) {
            createContainerCmd
                    .withEnv(inspectSelfResponse.getConfig().getEnv());
        }

        newHostConfig.withNanoCPUs(
                dockerUtils.getNanoCpusFromDecimalCpus(orchestrationProperties.docker().pgFacade().resources().cpuLimit())
        );
        newHostConfig.withMemory(
                dockerUtils.getMemoryBytesFromString(orchestrationProperties.docker().pgFacade().resources().memoryLimit())
        );

        createContainerCmd.withHostConfig(newHostConfig);
        CreateContainerResponse createContainerResponse = createContainerCmd.exec();

        for (var selfNetwork : inspectSelfResponse.getNetworkSettings().getNetworks().keySet()) {
            dockerClientManager.getDockerClient().connectToNetworkCmd()
                    .withContainerId(createContainerResponse.getId())
                    .withNetworkId(selfNetwork)
                    .exec();
        }

        dockerClientManager.getDockerClient().startContainerCmd(createContainerResponse.getId()).exec();
        InspectContainerResponse inspectNewContainerResponse = dockerClientManager.getDockerClient()
                .inspectContainerCmd(createContainerResponse.getId())
                .exec();

        return inspectContainerResponseToPgFacadeRaftNodeInfo(inspectNewContainerResponse);
    }

    @Override
    public void suspendPgFacadeInstance(String adapterIdentifier) throws PlatformAdapterOperationExecutionException {
        try {
            dockerClientManager.getDockerClient().renameContainerCmd(adapterIdentifier)
                    .withName(dockerUtils.createUniqueObjectName(DockerConstants.SUSPENDED_PG_FACADE_CONTAINER_NAME_PREFIX))
                    .exec();
        } catch (Exception e) {
            throw new PlatformAdapterOperationExecutionException("Failed to suspend container!", e);
        }
    }

    @Override
    public List<PgFacadeNodeHttpConnectionsInfo> getActivePgFacadeHttpNodesInfos() {
        List<Container> containers = listPgFacadeUnsuspendedContainers();

        if (CollectionUtils.isEmpty(containers)) {
            return Collections.emptyList();
        }

        return containers
                .stream()
                .map(this::containerToHttpNodeInfo)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public PgFacadeNodeExternalConnectionsInfo getSelfExternalConnectionInfo() {
        String hostname = System.getenv(DockerConstants.DOCKER_ENV_VAR_HOSTNAME);

        if (hostname == null) {
            throw new PlatformAdapterOperationExecutionException("Docker error. PgFacade container has no HOSTNAME env var. Container with PgFacade must have it, and this env var must contain Docker container ID start symbols.");
        }

        List<Container> containers = dockerClientManager.getDockerClient().listContainersCmd()
                .withLabelFilter(List.of(PgFacadeConstants.DOCKER_SPECIFIC_PGFACADE_CONTAINER_LABEL))
                .withIdFilter(List.of(hostname))
                .exec();

        return containerToExternalConnectionsInfo(containers.get(0));
    }

    @Override
    public boolean deletePgFacadeInstance(String adapterInstanceId) {
        if (adapterInstanceId == null) {
            return true;
        }

        try {
            InspectContainerResponse inspectContainerResponse = dockerClientManager.getDockerClient()
                    .inspectContainerCmd(adapterInstanceId)
                    .exec();

            try {
                dockerClientManager.getDockerClient().stopContainerCmd(adapterInstanceId).exec();
            } catch (Exception ignored) {
            }

            dockerClientManager.getDockerClient().removeContainerCmd(adapterInstanceId).withForce(true).withRemoveVolumes(true).exec();

            for (var bind : inspectContainerResponse.getHostConfig().getBinds()) {
                try {
                    dockerClientManager.getDockerClient().removeVolumeCmd(bind.getPath()).exec();
                } catch (Exception ignored) {
                    log.warn("Failed to remove volume {} for PgFacade instance", bind.getPath());
                }
            }

            return true;
        } catch (NotFoundException notFoundException) {
            return true;
        } catch (Exception e) {
            log.error("Failed to remove container with ID {} for PgFacade. Remove it manually.", adapterInstanceId, e);
            return false;
        }

    }

    private PgFacadeRaftNodeInfo inspectContainerResponseToPgFacadeRaftNodeInfo(InspectContainerResponse inspectContainerResponse) {
        if (inspectContainerResponse == null) {
            return null;
        }

        String address = dockerUtils.getContainerAddress(inspectContainerResponse, orchestrationProperties.docker().pgFacade().internalNetworkName());

        if (address == null) {
            return null;
        }

        return PgFacadeRaftNodeInfo
                .builder()
                .platformAdapterIdentifier(inspectContainerResponse.getId())
                .address(address)
                .createdWhen(Instant.parse(inspectContainerResponse.getCreated()))
                .port(PgFacadeConstants.DOCKER_SPECIFIC_PGFACADE_RAFT_PORT)
                .build();
    }

    private List<Container> listPgFacadeUnsuspendedContainers() {
        List<Container> containers = dockerClientManager.getDockerClient().listContainersCmd()
                .withLabelFilter(List.of(PgFacadeConstants.DOCKER_SPECIFIC_PGFACADE_CONTAINER_LABEL))
                .withStatusFilter(List.of(DockerConstants.ContainerState.RUNNING.getValue()))
                .exec();

        if (CollectionUtils.isEmpty(containers)) {
            return Collections.emptyList();
        }

        return containers.stream()
                .filter(container -> Arrays
                        .stream(container.getNames())
                        .noneMatch(name -> name.contains(DockerConstants.SUSPENDED_PG_FACADE_CONTAINER_NAME_PREFIX))
                )
                .collect(Collectors.toList());
    }

    private PgFacadeRaftNodeInfo containerToPgFacadeRaftNodeInfo(Container container) {
        if (container == null) {
            return null;
        }

        String address = dockerUtils.getContainerAddress(container, orchestrationProperties.docker().pgFacade().externalNetworkName());

        if (address == null) {
            return null;
        }

        return PgFacadeRaftNodeInfo
                .builder()
                .platformAdapterIdentifier(container.getId())
                .address(address)
                .createdWhen(Instant.ofEpochMilli(container.getCreated()))
                .port(PgFacadeConstants.DOCKER_SPECIFIC_PGFACADE_RAFT_PORT)
                .build();
    }

    private PgFacadeNodeExternalConnectionsInfo containerToExternalConnectionsInfo(Container container) {
        String address = dockerUtils.getContainerAddress(container, orchestrationProperties.docker().pgFacade().externalNetworkName());

        if (address == null) {
            return null;
        }

        return PgFacadeNodeExternalConnectionsInfo
                .builder()
                .address(address)
                .httpPort(pgFacadeRuntimeProperties.getHttpPort())
                .primaryPort(proxyProperties.primaryPort())
                .standbyPort(proxyProperties.standbyPort())
                .build();
    }

    private PgFacadeNodeHttpConnectionsInfo containerToHttpNodeInfo(Container container) {
        if (container == null) {
            return null;
        }

        String address = dockerUtils.getContainerAddress(container, orchestrationProperties.docker().pgFacade().externalNetworkName());

        if (address == null) {
            return null;
        }

        return PgFacadeNodeHttpConnectionsInfo
                .builder()
                .address(address)
                .port(pgFacadeRuntimeProperties.getHttpPort())
                .build();
    }
}
