package com.lantromipis.orchestration.adapter.impl;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.*;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.orchestration.adapter.api.OrchestrationAdapter;
import com.lantromipis.orchestration.constant.CommandsConstants;
import com.lantromipis.orchestration.constant.DockerConstants;
import com.lantromipis.orchestration.mapper.DockerMapper;
import com.lantromipis.orchestration.model.PostgresInstanceCreationRequest;
import com.lantromipis.orchestration.model.PostgresInstanceInfo;
import com.lantromipis.orchestration.util.PostgresUtils;
import com.lantromipis.orchestration.util.DockerUtils;
import io.quarkus.arc.lookup.LookupIfProperty;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

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

    @Inject
    PostgresUtils postgresUtils;

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

            String containerNamePostfix = UUID.randomUUID().toString();

            CreateContainerCmd createContainerCmd = dockerClient.createContainerCmd(dockerProperties.postgresImageTag())
                    .withName(dockerUtils.createUniqueObjectName(dockerProperties.postgresContainerName(), containerNamePostfix))
                    .withHostConfig(
                            HostConfig.newHostConfig()
                                    .withNetworkMode(dockerProperties.postgresNetworkName())
                    )
                    .withEnv(
                            List.of(
                                    createEnvValueForRequest(DockerConstants.POSTGRES_ENV_VAR_PASSWORD, postgresProperties.users().superuser().password()),
                                    createEnvValueForRequest(DockerConstants.POSTGRES_ENV_VAR_USERNAME, postgresProperties.users().superuser().username()),
                                    createEnvValueForRequest(DockerConstants.POSTGRES_ENV_VAR_DB, postgresProperties.users().superuser().database())
                            )
                    )
                    .withHealthcheck(
                            new HealthCheck()
                                    .withInterval(TimeUnit.MILLISECONDS.toNanos(dockerProperties.postgresHealthcheck().interval()))
                                    .withRetries(dockerProperties.postgresHealthcheck().retries())
                                    .withStartPeriod(TimeUnit.MILLISECONDS.toNanos(dockerProperties.postgresHealthcheck().startPeriod()))
                                    .withTimeout(TimeUnit.MILLISECONDS.toNanos(dockerProperties.postgresHealthcheck().timeout()))
                                    .withTest(List.of(DockerConstants.HEALTHCHECK_CMD_SHELL, dockerProperties.postgresHealthcheck().cmdShellCommand()))
                    );

            List<String> postgresqlSettings = createSettingsCmd(request.getPostgresqlSettings());

            if (!request.isMaster()) {
                String volumeName = createVolumeWithPgBaseBackup(containerNamePostfix);
                return null;
            }

            createContainerCmd.withCmd(postgresqlSettings);

            CreateContainerResponse createResponse = createContainerCmd.exec();

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

    @Override
    public boolean deletePostgresInstance(UUID instanceId) {
        String containerId = instanceIdAndContainerIdMap.get(instanceId);

        if (containerId == null) {
            return true;
        }

        try {
            dockerClient.removeContainerCmd(containerId);
            return true;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    private String createVolumeWithPgBaseBackup(String containerPostfix) {
        try {
            OrchestrationProperties.DockerProperties dockerProperties = orchestrationProperties.docker();

            CreateVolumeResponse createVolumeResponse = dockerClient.createVolumeCmd()
                    .withName(dockerUtils.createUniqueObjectName(dockerProperties.postgresVolumeName(), containerPostfix))
                    .exec();

            CreateContainerResponse tempCreateContainerResponse = dockerClient.createContainerCmd(dockerProperties.postgresImageTag())
                    .withName(dockerUtils.createUniqueObjectName(dockerProperties.helperObjectName()))
                    .withHostConfig(
                            HostConfig.newHostConfig()
                                    .withBinds(
                                            new Bind(
                                                    createVolumeResponse.getName(),
                                                    new Volume(DockerConstants.HELP_CONTAINER_BASE_BACKUP_PATH)
                                            )
                                    )
                                    .withNetworkMode(dockerProperties.postgresNetworkName())
                    )
                    //we only need Postgres utils like pg_basebackup and don't want to start DB itself
                    .withEntrypoint("sleep", "infinity")
                    .exec();

            dockerClient.startContainerCmd(tempCreateContainerResponse.getId()).exec();

            String commandToExecute = postgresUtils.getCommandToCreatePgPassFile(postgresProperties.users().replication()) + "; " + postgresUtils.createPgBaseBackupCommand(DockerConstants.HELP_CONTAINER_BASE_BACKUP_PATH);

            ExecCreateCmdResponse execCreateCmdResponse = dockerClient.execCreateCmd(tempCreateContainerResponse.getId())
                    .withCmd("/bin/sh", "-c", commandToExecute)
                    .exec();

            log.info("Waiting for backup.");

            dockerClient.execStartCmd(execCreateCmdResponse.getId())
                    .exec(new ResultCallback.Adapter<>())
                    .awaitCompletion();

            log.info("Finished waiting for backup.");

            dockerClient.stopContainerCmd(tempCreateContainerResponse.getId());
            dockerClient.removeContainerCmd(tempCreateContainerResponse.getId());

            return null;

        } catch (Exception e) {
            log.error("Error while creating volume with backup.", e);
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private List<String> createSettingsCmd(Map<String, String> settings) {
        if (MapUtils.isEmpty(settings)) {
            return Collections.emptyList();
        }

        List<String> ret = new LinkedList<>();

        ret.add(CommandsConstants.POSTGRES_COMMAND);

        for (var setting : settings.entrySet()) {
            ret.add(CommandsConstants.POSTGRES_COMMAND_PARAMETER_KEY);
            ret.add(setting.getKey() + "=" + setting.getValue());
        }

        return ret;
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
