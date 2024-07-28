package com.lantromipis.orchestration.adapter.impl;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.exception.NotModifiedException;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.HostConfig;
import com.lantromipis.configuration.properties.constant.ExternalLoadBalancerConstants;
import com.lantromipis.configuration.properties.constant.PgFacadeConstants;
import com.lantromipis.configuration.properties.constant.QuarkusConstants;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.orchestration.adapter.api.LoadBalancerPlatformAdapter;
import com.lantromipis.orchestration.adapter.api.PgFacadePlatformAdapter;
import com.lantromipis.orchestration.exception.PlatformAdapterNotFoundException;
import com.lantromipis.orchestration.exception.PlatformAdapterOperationExecutionException;
import com.lantromipis.orchestration.model.ExternalLoadBalancerAdapterInfo;
import com.lantromipis.orchestration.model.PgFacadeNodeExternalConnectionsInfo;
import com.lantromipis.orchestration.util.DockerUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@ApplicationScoped
public class DockerLoadBalancerPlatformAdapter implements LoadBalancerPlatformAdapter {

    @Inject
    DockerClientManager dockerClientManager;

    @Inject
    OrchestrationProperties orchestrationProperties;
    ;

    @Inject
    PgFacadePlatformAdapter pgFacadePlatformAdapter;

    @Inject
    DockerUtils dockerUtils;

    @Override
    public String createExternalLoadBalancerInstance() throws PlatformAdapterOperationExecutionException {
        try {
            PgFacadeNodeExternalConnectionsInfo selfInfo = pgFacadePlatformAdapter.getSelfExternalConnectionInfo();

            CreateContainerCmd createContainerCmd = dockerClientManager.getDockerClient().createContainerCmd(orchestrationProperties.docker().externalLoadBalancer().imageTag())
                    .withName(dockerUtils.createUniqueObjectName(orchestrationProperties.docker().externalLoadBalancer().containerName(), UUID.randomUUID().toString()))
                    .withLabels(Map.of(PgFacadeConstants.DOCKER_SPECIFIC_EXTERNAL_LOAD_BALANCER_CONTAINER_LABEL, "true"))
                    .withEnv(
                            List.of(
                                    dockerUtils.createEnvValueForRequest(ExternalLoadBalancerConstants.ENV_INITIAL_HTTP_HOST, selfInfo.getAddress()),
                                    dockerUtils.createEnvValueForRequest(ExternalLoadBalancerConstants.ENV_INITIAL_HTTP_PORT, String.valueOf(selfInfo.getHttpPort())),
                                    dockerUtils.createEnvValueForRequest(QuarkusConstants.ENV_QUARKUS_HTTP_PORT, String.valueOf(QuarkusConstants.DEFAULT_HTTP_PORT))
                            )
                    ).withHostConfig(
                            HostConfig.newHostConfig()
                                    .withNanoCPUs(dockerUtils.getNanoCpusFromDecimalCpus(orchestrationProperties.docker().externalLoadBalancer().resources().cpuLimit()))
                                    .withMemory(dockerUtils.getMemoryBytesFromString(orchestrationProperties.docker().externalLoadBalancer().resources().memoryLimit()))
                    );

            CreateContainerResponse createContainerResponse = createContainerCmd.exec();

            dockerClientManager.getDockerClient().connectToNetworkCmd()
                    .withContainerId(createContainerResponse.getId())
                    .withNetworkId(orchestrationProperties.docker().pgFacade().externalNetworkName())
                    .withContainerNetwork(
                            new ContainerNetwork()
                                    .withAliases(orchestrationProperties.docker().externalLoadBalancer().dnsAlias())
                    )
                    .exec();

            dockerClientManager.getDockerClient().connectToNetworkCmd()
                    .withContainerId(createContainerResponse.getId())
                    .withNetworkId(orchestrationProperties.docker().externalLoadBalancer().networkForEndClients())
                    .withContainerNetwork(
                            new ContainerNetwork()
                                    .withAliases(orchestrationProperties.docker().externalLoadBalancer().dnsAlias())
                    )
                    .exec();

            return createContainerResponse.getId();
        } catch (PlatformAdapterOperationExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new PlatformAdapterOperationExecutionException("Failed to create container with load balancer!", e);
        }
    }

    @Override
    public ExternalLoadBalancerAdapterInfo getExternalLoadBalancerInstanceInfo(String adapterIdentifier) throws PlatformAdapterOperationExecutionException {
        try {
            InspectContainerResponse inspectContainerResponse = dockerClientManager.getDockerClient()
                    .inspectContainerCmd(adapterIdentifier)
                    .exec();
            return from(inspectContainerResponse);
        } catch (Exception e) {
            throw new PlatformAdapterOperationExecutionException("Failed to get info about external load balancer.", e);
        }
    }

    @Override
    public ExternalLoadBalancerAdapterInfo startExternalLoadBalancerInstance(String adapterIdentifier) throws PlatformAdapterOperationExecutionException {
        try {
            dockerClientManager.getDockerClient().startContainerCmd(adapterIdentifier).exec();
            InspectContainerResponse inspectContainerResponse = dockerClientManager.getDockerClient()
                    .inspectContainerCmd(adapterIdentifier)
                    .exec();
            return from(inspectContainerResponse);
        } catch (Exception e) {
            throw new PlatformAdapterOperationExecutionException("Failed to start external load balancer.", e);
        }
    }

    @Override
    public boolean stopExternalLoadBalancerInstance(String adapterIdentifier) {
        if (adapterIdentifier == null) {
            throw new PlatformAdapterNotFoundException("Can not stop container with load balancer because container ID is null.");
        }

        try {
            dockerClientManager.getDockerClient().stopContainerCmd(adapterIdentifier).exec();
            return true;
        } catch (NotFoundException notFoundException) {
            log.warn("Failed to stop load balancer. Container with ID " + adapterIdentifier + " not found.");
            return true;
        } catch (NotModifiedException notModifiedException) {
            return true;
        } catch (Exception e) {
            log.error("Unexpected error while stopping load balancer with container ID {}", adapterIdentifier, e);
            return false;
        }
    }

    @Override
    public boolean deleteExternalLoadBalancerInstance(String adapterInstanceId) {
        if (adapterInstanceId == null) {
            return true;
        }

        try {
            try {
                dockerClientManager.getDockerClient().stopContainerCmd(adapterInstanceId).exec();
            } catch (Exception ignored) {
            }

            dockerClientManager.getDockerClient().removeContainerCmd(adapterInstanceId)
                    .withForce(true)
                    .withRemoveVolumes(true)
                    .exec();

            return true;
        } catch (NotFoundException notFoundException) {
            return true;
        } catch (Exception e) {
            log.error("Failed to remove container with ID {} for external load balancer. Remove it manually.", adapterInstanceId, e);
            return false;
        }

    }

    private ExternalLoadBalancerAdapterInfo from(InspectContainerResponse inspectContainerResponse) {
        return ExternalLoadBalancerAdapterInfo
                .builder()
                .running(Boolean.TRUE.equals(inspectContainerResponse.getState().getRunning()))
                .adapterIdentifier(inspectContainerResponse.getId())
                .httpPort(QuarkusConstants.DEFAULT_HTTP_PORT)
                .address(dockerUtils.getContainerAddress(inspectContainerResponse, orchestrationProperties.docker().pgFacade().externalNetworkName()))
                .build();
    }
}
