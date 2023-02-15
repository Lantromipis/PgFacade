package com.lantromipis.orchestration.adapter.impl;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.*;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.lantromipis.configuration.model.PostgresPersistedNodeInfo;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.configuration.properties.stored.api.PersistedProperties;
import com.lantromipis.orchestration.adapter.api.OrchestrationAdapter;
import com.lantromipis.orchestration.constant.CommandsConstants;
import com.lantromipis.orchestration.constant.DockerConstants;
import com.lantromipis.orchestration.constant.PostgresConstant;
import com.lantromipis.orchestration.exception.DockerEnvironmentConfigurationException;
import com.lantromipis.orchestration.mapper.DockerMapper;
import com.lantromipis.orchestration.model.PostgresInstanceCreationRequest;
import com.lantromipis.orchestration.model.PostgresAdapterInstanceInfo;
import com.lantromipis.orchestration.util.PostgresUtils;
import com.lantromipis.orchestration.util.DockerUtils;
import io.quarkus.arc.lookup.LookupIfProperty;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
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

    @Inject
    PersistedProperties persistedProperties;

    private DockerClient dockerClient;

    private String postgresNetworkId;

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
        //TODO need to sync with docker by calling listContainers?
        getAvailablePostgresInstancesInfos();

        log.info("Successfully created Docker client for cluster management.");
    }

    @Override
    public UUID createNewPostgresInstance(PostgresInstanceCreationRequest request) {
        //used to delete container if it was created but method failed.
        String containerId = null;
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

            if (!request.isMaster()) {
                String volumeName = createVolumeWithPgBaseBackup(containerNamePostfix);
                if (volumeName == null) {
                    log.error("Unable to create stand-by because volume creation with backup failed.");
                    return null;
                }
                createContainerCmd.getHostConfig()
                        .withBinds(
                                new Bind(
                                        volumeName,
                                        new Volume(orchestrationProperties.docker().postgresImagePgDataDir())
                                )
                        );
                Map<String, String> standBySettings = new HashMap<>(request.getPostgresqlSettings());
                standBySettings.put(PostgresConstant.PRIMARY_CONN_INFO_SETTING, postgresUtils.getPrimaryConnInfoSetting());

                createContainerCmd.withCmd(createSettingsCmd(standBySettings));
            } else {
                createContainerCmd.withCmd(createSettingsCmd(request.getPostgresqlSettings()));
            }

            CreateContainerResponse createResponse = createContainerCmd.exec();
            containerId = createResponse.getId();

            UUID instanceId = rememberContainer(containerId);

            persistedProperties.savePostgresNodeInfo(
                    PostgresPersistedNodeInfo
                            .builder()
                            .master(request.isMaster())
                            .instanceId(instanceId)
                            .adapterIdentifier(containerId)
                            .build()
            );

            if (request.isMaster()) {
                log.info("Created container with master. Ready to start it.");
            } else {
                log.info("Created container with stand-by. Ready to start it.");
            }

            return instanceId;

        } catch (Exception e) {
            log.error("Failed to create new container ", e);
            try {
                if (containerId != null) {
                    dockerClient.removeContainerCmd(containerId);
                    forgetContainer(containerId);
                }
            } catch (Exception e2) {
                log.error("Error occurred after container was created, but it is impossible to delete it. Container id = {}", containerId, e2);
            }
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
            //TODO check if instance is running?
            dockerClient.startContainerCmd(containerId).exec();
            return true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    @Override
    public PostgresAdapterInstanceInfo getInstanceInfo(UUID instanceId) {
        String containerId = instanceIdAndContainerIdMap.get(instanceId);

        if (containerId == null) {
            return null;
        }

        try {
            InspectContainerResponse inspectResponse = dockerClient.inspectContainerCmd(containerId).exec();
            PostgresPersistedNodeInfo persistedNodeInfo = persistedProperties.getPostgresNodeInfo(instanceId);

            return PostgresAdapterInstanceInfo
                    .builder()
                    .instanceId(instanceId)
                    .instanceAddress(dockerUtils.getContainerAddress(inspectResponse))
                    .instancePort(5432) //TODO maybe need to change
                    .status(dockerMapper.toInstanceStatus(inspectResponse.getState().getStatus()))
                    .health(dockerMapper.toInstanceHealth(inspectResponse))
                    .master(persistedNodeInfo.isMaster())
                    .build();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public List<PostgresAdapterInstanceInfo> getAvailablePostgresInstancesInfos() {
        //method almost duplicates getInstanceInfo(UUID) for performance, because here we get persisted info in one call
        List<PostgresAdapterInstanceInfo> ret = new ArrayList<>();

        for (PostgresPersistedNodeInfo persistedNodeInfo : persistedProperties.getPostgresNodeInfos()) {
            try {
                InspectContainerResponse inspectResponse = dockerClient.inspectContainerCmd(persistedNodeInfo.getAdapterIdentifier()).exec();

                ret.add(
                        PostgresAdapterInstanceInfo
                                .builder()
                                .instanceId(persistedNodeInfo.getInstanceId())
                                .instanceAddress(dockerUtils.getContainerAddress(inspectResponse))
                                .instancePort(5432)
                                .status(dockerMapper.toInstanceStatus(inspectResponse.getState().getStatus()))
                                .health(dockerMapper.toInstanceHealth(inspectResponse))
                                .master(persistedNodeInfo.isMaster())
                                .build()
                );
            } catch (NotFoundException e) {
                persistedProperties.deletePostgresNodeInfo(persistedNodeInfo.getInstanceId());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        return ret;
    }

    @Override
    public boolean deletePostgresInstance(UUID instanceId) {
        String containerId = instanceIdAndContainerIdMap.get(instanceId);

        if (containerId == null) {
            return true;
        }

        try {
            forgetContainer(containerId);
            dockerClient.removeContainerCmd(containerId);
            return true;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    @Override
    public void updateInstancesAfterSwitchover(UUID newMasterInstanceId, UUID oldMasterInstanceId) {
        PostgresPersistedNodeInfo newMasterPersistedInfo = persistedProperties.getPostgresNodeInfo(newMasterInstanceId);
        newMasterPersistedInfo.setMaster(true);
        persistedProperties.savePostgresNodeInfo(newMasterPersistedInfo);
        deletePostgresInstance(oldMasterInstanceId);
    }

    @Override
    public List<String> getRequiredHbaConfLines() {
        List<Network> networks = dockerClient.listNetworksCmd()
                .withNameFilter(orchestrationProperties.docker().postgresNetworkName())
                .exec();

        if (CollectionUtils.isEmpty(networks) || networks.size() > 1) {
            throw new DockerEnvironmentConfigurationException("Found no or several networks for Postgres. There must be only one network for Postgres and this network must have unique name because filtering by name must return only one network.");
        }

        Network pgFacadePostgresNetwork = networks.get(0);
        if (CollectionUtils.isEmpty(pgFacadePostgresNetwork.getIpam().getConfig()) || pgFacadePostgresNetwork.getIpam().getConfig().size() > 1) {
            throw new DockerEnvironmentConfigurationException("Found no or several IPAM config for Postgres network. Network must have exactly one IPAM config with subnet in it.");
        }

        String postgresSubnet = pgFacadePostgresNetwork.getIpam().getConfig().get(0).getSubnet();

        List<String> result = new ArrayList<>();

        result.add(PostgresConstant.PG_HBA_CONF_START_LINE);

        //for superuser. For security reasons, default is local
        result.add(postgresUtils.generatePgHbaConfLine(
                        PostgresConstant.PgHbaConfHost.LOCAL,
                        PostgresConstant.PG_HBA_CONF_ALL,
                        postgresProperties.users().superuser().username(),
                        postgresSubnet,
                        PostgresConstant.PgHbaConfAuthMethod.SCRAM_SHA_256
                )
        );

        //for PgFacade user
        result.add(postgresUtils.generatePgHbaConfLine(
                        PostgresConstant.PgHbaConfHost.HOST,
                        postgresProperties.users().pgFacade().database(),
                        postgresProperties.users().pgFacade().username(),
                        postgresSubnet,
                        PostgresConstant.PgHbaConfAuthMethod.SCRAM_SHA_256
                )
        );

        //for healthcheck user
        //TODO add healthcheck user
        result.add(postgresUtils.generatePgHbaConfLine(
                        PostgresConstant.PgHbaConfHost.LOCAL,
                        postgresProperties.users().pgFacade().database(),
                        postgresProperties.users().pgFacade().username(),
                        postgresSubnet,
                        PostgresConstant.PgHbaConfAuthMethod.SCRAM_SHA_256
                )
        );

        //for replication user
        result.add(postgresUtils.generatePgHbaConfLine(
                        PostgresConstant.PgHbaConfHost.HOST,
                        PostgresConstant.PG_HBA_CONF_REPLICATION_DB,
                        postgresProperties.users().replication().username(),
                        postgresSubnet,
                        PostgresConstant.PgHbaConfAuthMethod.SCRAM_SHA_256
                )
        );

        return result;
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

            String commandToExecute = postgresUtils.getCommandToCreatePgPassFile(postgresProperties.users().replication())
                    + " ; " + postgresUtils.createPgBaseBackupCommand(DockerConstants.HELP_CONTAINER_BASE_BACKUP_PATH)
                    + " ; touch " + DockerConstants.HELP_CONTAINER_BASE_BACKUP_PATH + "/standby.signal";

            log.info("Creating backup for standby. This will take some time...");

            ExecCreateCmdResponse backupExecCreateResponse = dockerClient.execCreateCmd(tempCreateContainerResponse.getId())
                    .withAttachStdout(true)
                    .withAttachStderr(true)
                    .withCmd("/bin/sh", "-c", commandToExecute)
                    .exec();

            //a little hack because of bug in docker-java lib. Attaching stdout will make this call synchronous
            ByteArrayOutputStream stdOut = new ByteArrayOutputStream();
            ByteArrayOutputStream stdErr = new ByteArrayOutputStream();

            dockerClient.execStartCmd(backupExecCreateResponse.getId())
                    .withDetach(false)
                    .exec(new ExecStartResultCallback(stdOut, stdErr))
                    .awaitCompletion();

            InspectExecResponse inspectBackupExecResponse = dockerClient.inspectExecCmd(backupExecCreateResponse.getId()).exec();

            if (inspectBackupExecResponse.getExitCodeLong() != 0) {
                log.error("Error while creating backup. Message from CMD: {}", stdErr);
                return null;
            }

            log.info("Finished creating backup for standby.");

            dockerClient.stopContainerCmd(tempCreateContainerResponse.getId()).exec();
            dockerClient.removeContainerCmd(tempCreateContainerResponse.getId()).exec();

            return createVolumeResponse.getName();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while creating volume with backup.", e);
            return null;
        } catch (Exception e) {
            log.error("Error while creating volume with backup.", e);
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
        UUID existingInstanceId = containerIdAndInstanceIdMap.get(containerId);
        if (existingInstanceId != null) {
            return existingInstanceId;
        }

        UUID instanceId = UUID.randomUUID();

        instanceIdAndContainerIdMap.put(instanceId, containerId);
        containerIdAndInstanceIdMap.put(containerId, instanceId);

        return instanceId;
    }

    private void forgetContainer(String containerId) {
        UUID instanceId = containerIdAndInstanceIdMap.remove(containerId);
        if (instanceId != null) {
            instanceIdAndContainerIdMap.remove(instanceId);
        }
        persistedProperties.deletePostgresNodeInfo(instanceId);
    }

    private void forgetContainer(UUID instanceId) {
        String containerId = instanceIdAndContainerIdMap.remove(instanceId);
        if (containerId != null) {
            containerIdAndInstanceIdMap.remove(containerId);
        }
    }

    private String createEnvValueForRequest(String varName, String value) {
        return varName + "=" + value;
    }
}
