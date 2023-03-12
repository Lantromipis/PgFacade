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
import com.lantromipis.configuration.properties.constant.PostgresqlConfConstants;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.configuration.properties.stored.api.PostgresPersistedProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.constant.CommandsConstants;
import com.lantromipis.orchestration.constant.DockerConstants;
import com.lantromipis.orchestration.constant.PostgresConstants;
import com.lantromipis.orchestration.exception.DockerEnvironmentConfigurationException;
import com.lantromipis.orchestration.exception.PostgresRestoreException;
import com.lantromipis.orchestration.mapper.DockerMapper;
import com.lantromipis.orchestration.model.AdapterShellCommandExecutionResult;
import com.lantromipis.orchestration.model.BaseBackupCreationResult;
import com.lantromipis.orchestration.model.PostgresInstanceCreationRequest;
import com.lantromipis.orchestration.model.PostgresAdapterInstanceInfo;
import com.lantromipis.orchestration.util.PgFacadeIOUtils;
import com.lantromipis.orchestration.util.PostgresUtils;
import com.lantromipis.orchestration.util.DockerUtils;
import io.quarkus.arc.lookup.LookupIfProperty;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.input.ObservableInputStream;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;

@Slf4j
@ApplicationScoped
@LookupIfProperty(name = "pg-facade.orchestration.adapter", stringValue = "docker")
public class DockerBasedPlatformAdapter implements PlatformAdapter {

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
    PostgresPersistedProperties persistedProperties;

    @Inject
    ManagedExecutor managedExecutor;

    @Inject
    PgFacadeIOUtils pgFacadeIOUtils;

    //TODO add timeouts for client
    private DockerClient dockerClient;

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
    public void shutdown() {
        try {
            dockerClient.close();
        } catch (Exception ignored) {
        }
    }

    @Override
    public UUID createNewPostgresInstance(PostgresInstanceCreationRequest request) {
        //used to delete container if it was created but method failed.
        String containerId = null;
        UUID instanceId = null;
        try {
            UUID futureInstanceId = UUID.randomUUID();
            String containerNamePostfix = futureInstanceId.toString();

            CreateContainerCmd createContainerCmd = getDefaultCreateContainerCmdRequest(containerNamePostfix);

            if (!request.isPrimary()) {
                Map<String, String> postgresSettings = new HashMap<>(request.getStandbySettings());
                postgresSettings.put(PostgresConstants.PRIMARY_CONN_INFO_SETTING_NAME, postgresUtils.getPrimaryConnInfoSetting());

                String volumeName = createVolumeWithPgBaseBackupForStandby(containerNamePostfix, postgresSettings);

                if (volumeName == null) {
                    log.error("Unable to create stand-by because volume creation with backup failed.");
                    return null;
                }
                createContainerCmd.getHostConfig()
                        .withBinds(
                                new Bind(
                                        volumeName,
                                        new Volume(orchestrationProperties.docker().postgres().imagePgData())
                                )
                        );
            }

            CreateContainerResponse createResponse = createContainerCmd.exec();
            containerId = createResponse.getId();

            instanceId = futureInstanceId;

            persistedProperties.savePostgresNodeInfo(
                    PostgresPersistedNodeInfo
                            .builder()
                            .primary(request.isPrimary())
                            .instanceId(instanceId)
                            .adapterIdentifier(containerId)
                            .build()
            );

            if (request.isPrimary()) {
                log.info("Created container with primary. Ready to start it.");
            } else {
                log.info("Created container with stand-by. Ready to start it.");
            }

            return instanceId;

        } catch (Exception e) {
            log.error("Failed to create new container ", e);
            try {
                forceDeleteContainerSafe(containerId);
                if (instanceId != null) {
                    persistedProperties.deletePostgresNodeInfo(instanceId);
                }
            } catch (Exception e2) {
                log.error("Error occurred after container was created, but it is impossible to delete it. Container id = {}", containerId, e2);
            }
            return null;
        }
    }

    @Override
    public boolean startPostgresInstance(UUID instanceId) {
        String containerId = instanceIdToContainerId(instanceId);

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
    public boolean stopPostgresInstance(UUID instanceId) {
        String containerId = instanceIdToContainerId(instanceId);

        if (containerId == null) {
            return true;
        }

        try {
            dockerClient.stopContainerCmd(containerId).exec();
            return true;
        } catch (Exception e) {
            log.error("Error stopping Postgres instance", e);
            return false;
        }
    }

    @Override
    public boolean restartPostgresInstance(UUID instanceId) {
        String containerId = instanceIdToContainerId(instanceId);

        if (containerId == null) {
            return false;
        }

        try {
            dockerClient.restartContainerCmd(containerId).exec();
            return true;
        } catch (Exception e) {
            log.error("Error restarting Postgres instance", e);
            return false;
        }
    }

    @Override
    public PostgresAdapterInstanceInfo getInstanceInfo(UUID instanceId) {
        try {
            PostgresPersistedNodeInfo persistedNodeInfo = persistedProperties.getPostgresNodeInfo(instanceId);
            InspectContainerResponse inspectResponse = dockerClient.inspectContainerCmd(persistedNodeInfo.getAdapterIdentifier()).exec();

            return PostgresAdapterInstanceInfo
                    .builder()
                    .instanceId(instanceId)
                    .instanceAddress(dockerUtils.getContainerAddress(inspectResponse))
                    .instancePort(5432) //TODO maybe need to change
                    .status(dockerMapper.toInstanceStatus(inspectResponse.getState().getStatus()))
                    .health(dockerMapper.toInstanceHealth(inspectResponse))
                    .primary(persistedNodeInfo.isPrimary())
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
                                .primary(persistedNodeInfo.isPrimary())
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
    public boolean deletePostgresInstance(UUID instanceId, boolean force) {
        String containerId = instanceIdToContainerId(instanceId);

        if (containerId == null) {
            return true;
        }

        try {
            InspectContainerResponse inspectContainerResponse = dockerClient.inspectContainerCmd(containerId).exec();

            for (var bind : inspectContainerResponse.getHostConfig().getBinds()) {
                try {
                    dockerClient.removeVolumeCmd(bind.getPath()).exec();
                } catch (Exception ignored) {
                }
            }

            if (force) {
                forceDeleteContainerSafe(containerId);
            } else {
                try {
                    dockerClient.stopContainerCmd(containerId).exec();
                } catch (Exception ignored) {
                }

                dockerClient.removeContainerCmd(containerId).withRemoveVolumes(true).exec();
            }
            persistedProperties.deletePostgresNodeInfo(instanceId);
            return true;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    @Override
    public void updateInstancesAfterSwitchover(UUID newPrimaryInstanceId, UUID oldPrimaryInstanceId) {
        PostgresPersistedNodeInfo newPrimaryPersistedInfo = persistedProperties.getPostgresNodeInfo(newPrimaryInstanceId);
        newPrimaryPersistedInfo.setPrimary(true);
        persistedProperties.savePostgresNodeInfo(newPrimaryPersistedInfo);
        deletePostgresInstance(oldPrimaryInstanceId, true);
        log.info("Updated instances infos after failover. Previous container with primary deleted. New primary container id is {}", newPrimaryPersistedInfo.getAdapterIdentifier());
    }

    @Override
    public AdapterShellCommandExecutionResult executeShellCommandForInstance(UUID instanceId, String shellCommand, List<Long> okExitCodes) {
        String containerId = instanceIdToContainerId(instanceId);

        if (containerId == null) {
            return AdapterShellCommandExecutionResult
                    .builder()
                    .success(false)
                    .build();
        }

        return executeCmdInContainer(containerId, shellCommand, okExitCodes, null);
    }

    @Override
    public List<String> getRequiredHbaConfLines() {
        List<Network> networks = dockerClient.listNetworksCmd()
                .withNameFilter(orchestrationProperties.docker().postgres().networkName())
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

        result.add(PostgresConstants.PG_HBA_CONF_START_LINE);

        //for superuser. For security reasons, default is local
        result.add(postgresUtils.generatePgHbaConfLine(
                        PostgresConstants.PgHbaConfHost.LOCAL,
                        PostgresConstants.PG_HBA_CONF_ALL,
                        postgresProperties.users().superuser().username(),
                        postgresSubnet,
                        PostgresConstants.PgHbaConfAuthMethod.SCRAM_SHA_256
                )
        );

        //for PgFacade user
        result.add(postgresUtils.generatePgHbaConfLine(
                        PostgresConstants.PgHbaConfHost.HOST,
                        postgresProperties.users().pgFacade().database(),
                        postgresProperties.users().pgFacade().username(),
                        postgresSubnet,
                        PostgresConstants.PgHbaConfAuthMethod.SCRAM_SHA_256
                )
        );

        //for healthcheck user
        //TODO add healthcheck user
        result.add(postgresUtils.generatePgHbaConfLine(
                        PostgresConstants.PgHbaConfHost.LOCAL,
                        postgresProperties.users().pgFacade().database(),
                        postgresProperties.users().pgFacade().username(),
                        postgresSubnet,
                        PostgresConstants.PgHbaConfAuthMethod.SCRAM_SHA_256
                )
        );

        //for replication user
        result.add(postgresUtils.generatePgHbaConfLine(
                        PostgresConstants.PgHbaConfHost.HOST,
                        PostgresConstants.PG_HBA_CONF_REPLICATION_DB,
                        postgresProperties.users().replication().username(),
                        postgresSubnet,
                        PostgresConstants.PgHbaConfAuthMethod.SCRAM_SHA_256
                )
        );

        return result;
    }

    @Override
    public BaseBackupCreationResult createBaseBackupAndGetAsStream() {
        OrchestrationProperties.DockerProperties dockerProperties = orchestrationProperties.docker();

        CreateContainerResponse tempCreateContainerResponse = dockerClient.createContainerCmd(dockerProperties.postgres().imageTag())
                .withName(dockerUtils.createUniqueObjectName(dockerProperties.helperObjectName()))
                .withHostConfig(
                        HostConfig.newHostConfig()
                                .withNetworkMode(dockerProperties.postgres().networkName())
                )
                //we only need Postgres utils like pg_basebackup and don't want to start DB itself
                .withEntrypoint("sleep", "infinity")
                .exec();

        String containerId = tempCreateContainerResponse.getId();
        dockerClient.startContainerCmd(containerId).exec();

        try {
            String commandToExecute = postgresUtils.getCommandToCreatePgPassFileForPrimary(postgresProperties.users().replication())
                    + " ; " + postgresUtils.createPgBaseBackupCommand(DockerConstants.HELP_CONTAINER_BASE_BACKUP_PATH)
                    + " ; cat " + DockerConstants.HELP_CONTAINER_BASE_BACKUP_PATH + "/" + CommandsConstants.PG_BASE_BACKUP_BACKUP_LABEL_FILE_NAME;

            AdapterShellCommandExecutionResult baseBackupCommandExecutionResult = executeCmdInContainer(
                    containerId,
                    commandToExecute,
                    List.of(0L),
                    null
            );

            if (!baseBackupCommandExecutionResult.isSuccess()) {
                log.error("Failed to create backup as stream. Cause from CMD: {}", baseBackupCommandExecutionResult.getStderr());
                dockerClient.removeContainerCmd(containerId).withForce(true).withRemoveVolumes(true).exec();
                return BaseBackupCreationResult
                        .builder()
                        .success(false)
                        .build();
            }

            ObservableInputStream ret = new ObservableInputStream(
                    dockerClient.copyArchiveFromContainerCmd(
                                    containerId,
                                    DockerConstants.HELP_CONTAINER_BASE_BACKUP_PATH + "/"
                            )
                            .exec()
            );

            ret.add(new ObservableInputStream.Observer() {
                @Override
                public void closed() throws IOException {
                    forceDeleteContainerSafe(tempCreateContainerResponse.getId());
                    super.closed();
                }
            });

            String backupLabelContentWithoutLineBreaks = baseBackupCommandExecutionResult.getStdout().replaceAll("\n", "");
            Matcher matcher = CommandsConstants.PG_BASE_BACKUP_BACKUP_LABEL_WAL_FILE_NAME_PATTERN.matcher(backupLabelContentWithoutLineBreaks);
            String firstWalFileName;
            if (matcher.matches()) {
                firstWalFileName = matcher.group(1);
            } else {
                firstWalFileName = "";
                log.error("Failed to parse backup_label! Did backup_label file format change?");
            }

            return BaseBackupCreationResult
                    .builder()
                    .success(true)
                    .stream(ret)
                    .firstWalFileName(firstWalFileName)
                    .build();

        } catch (Exception e) {
            log.error("Error creating backup as stream ", e);
            forceDeleteContainerSafe(tempCreateContainerResponse.getId());
            return BaseBackupCreationResult
                    .builder()
                    .success(false)
                    .build();
        }
    }

    @Override
    public UUID restorePrimaryFromBackup(InputStream basebackupTarInputStream, List<String> walFileNames, Function<String, InputStream> walFileInputStreamFunction) throws PostgresRestoreException {
        OrchestrationProperties.DockerProperties dockerProperties = orchestrationProperties.docker();

        String containerNamePostfix = UUID.randomUUID().toString();
        String recoveryContainerId = null, volumeName = null;

        try {
            CreateVolumeResponse createVolumeResponse = dockerClient.createVolumeCmd()
                    .withName(dockerUtils.createUniqueObjectName(dockerProperties.postgres().volumeName(), containerNamePostfix))
                    .exec();

            volumeName = createVolumeResponse.getName();

            CreateContainerResponse tempCreateContainerResponse = dockerClient.createContainerCmd(dockerProperties.postgres().imageTag())
                    .withName(dockerUtils.createUniqueObjectName(dockerProperties.helperObjectName()))
                    .withHostConfig(
                            HostConfig.newHostConfig()
                                    .withBinds(
                                            new Bind(
                                                    createVolumeResponse.getName(),
                                                    new Volume(DockerConstants.HELP_CONTAINER_RESTORE_PGDATA_PATH)
                                            )
                                    )
                                    .withNetworkMode(dockerProperties.postgres().networkName())
                    )
                    // do not execute any default entrypoint scripts
                    .withEntrypoint("sh", "-c", "mkdir -p " + DockerConstants.HELP_CONTAINER_RESTORE_PGDATA_PATH + " " + DockerConstants.HELP_CONTAINER_RESTORE_WAL_PATH + " ; sleep infinity")
                    .exec();

            recoveryContainerId = tempCreateContainerResponse.getId();

            dockerClient.startContainerCmd(recoveryContainerId).exec();

            // copy backup
            try (basebackupTarInputStream) {
                dockerClient.copyArchiveToContainerCmd(recoveryContainerId)
                        .withRemotePath(DockerConstants.HELP_CONTAINER_RESTORE_PGDATA_PATH)
                        .withTarInputStream(basebackupTarInputStream)
                        .exec();
            }

            AdapterShellCommandExecutionResult findPgWalCommandResult = executeCmdInContainer(
                    recoveryContainerId,
                    "find " + DockerConstants.HELP_CONTAINER_RESTORE_ROOT_PATH + " -type d -name pg_wal",
                    List.of(0L),
                    null
            );
            if (!findPgWalCommandResult.isSuccess() || StringUtils.isEmpty(findPgWalCommandResult.getStdout())) {
                throw new PostgresRestoreException("Recovery failed! Failed to find pg_wal in container for restore. Is backup valid?");
            }

            String backupRootDir = findPgWalCommandResult.getStdout().replaceAll("\n", "").replaceAll("/pg_wal$", "");

            // adaptation for cases when .tar contains directory with pg_data, but not contents of pg_data itself
            if (!backupRootDir.equals(DockerConstants.HELP_CONTAINER_RESTORE_PGDATA_PATH)) {
                AdapterShellCommandExecutionResult mvBackupCommandResult = executeCmdInContainer(
                        recoveryContainerId,
                        "mv " + backupRootDir + "/*" + " " + DockerConstants.HELP_CONTAINER_RESTORE_PGDATA_PATH,
                        List.of(0L),
                        null
                );
                if (!mvBackupCommandResult.isSuccess()) {
                    throw new PostgresRestoreException("Recovery failed! Failed to move backup files inside container to restore dir. Cause from container shell: " + mvBackupCommandResult.getStderr());
                }
            }

            // copy WAL files
            for (String walFilename : walFileNames) {
                try (InputStream inputStream =
                             pgFacadeIOUtils.createInputStreamWithTarFromInputStreamContainingFile(
                                     walFilename,
                                     walFileInputStreamFunction.apply(walFilename)
                             )
                ) {
                    dockerClient.copyArchiveToContainerCmd(recoveryContainerId)
                            .withRemotePath(DockerConstants.HELP_CONTAINER_RESTORE_WAL_PATH)
                            .withTarInputStream(inputStream)
                            .exec();
                }
            }

            // clear pg_wal and set permissions
            AdapterShellCommandExecutionResult prepareForRecoveryCommandResult = executeCmdInContainer(
                    recoveryContainerId,
                    postgresUtils.getShellCommandToPrepareForRecovery(DockerConstants.HELP_CONTAINER_RESTORE_PGDATA_PATH),
                    List.of(0L),
                    null
            );

            if (!prepareForRecoveryCommandResult.isSuccess()) {
                throw new PostgresRestoreException("Recovery failed! Failed to prepare for recovery. Cause from container shell: " + prepareForRecoveryCommandResult.getStderr());
            }

            AdapterShellCommandExecutionResult startRecoveryCommandResult = executeCmdInContainer(
                    recoveryContainerId,
                    postgresUtils.getShellPgCtlToStartRecovery(DockerConstants.HELP_CONTAINER_RESTORE_PGDATA_PATH, DockerConstants.HELP_CONTAINER_RESTORE_WAL_PATH),
                    List.of(0L),
                    "postgres"
            );

            if (!startRecoveryCommandResult.isSuccess()) {
                throw new PostgresRestoreException("Recovery failed! Started recovery, but it failed! Cause from container shell: " + prepareForRecoveryCommandResult.getStderr());
            }

            AdapterShellCommandExecutionResult stopRecoveredPostgresResult = executeCmdInContainer(
                    recoveryContainerId,
                    postgresUtils.getShellPgCtlToStopPostgres(DockerConstants.HELP_CONTAINER_RESTORE_PGDATA_PATH),
                    List.of(0L),
                    "postgres"
            );

            if (!stopRecoveredPostgresResult.isSuccess()) {
                throw new PostgresRestoreException("Recovery failed! Failed to stop recovered postgres! Cause from container shell: " + stopRecoveredPostgresResult.getStderr());
            }

            dockerClient.stopContainerCmd(recoveryContainerId);
            forceDeleteContainerSafe(recoveryContainerId);

            log.info("Recovery completed successfully!");

            UUID instanceId = UUID.randomUUID();

            CreateContainerCmd createContainerCmd = getDefaultCreateContainerCmdRequest(instanceId.toString());

            createContainerCmd.getHostConfig()
                    .withBinds(
                            new Bind(
                                    volumeName,
                                    new Volume(orchestrationProperties.docker().postgres().imagePgData())
                            )
                    );

            CreateContainerResponse createContainerResponse = createContainerCmd.exec();

            persistedProperties.savePostgresNodeInfo(
                    PostgresPersistedNodeInfo
                            .builder()
                            .primary(true)
                            .instanceId(instanceId)
                            .adapterIdentifier(createContainerResponse.getId())
                            .build()
            );

            return instanceId;
        } catch (Exception e) {
            log.error("Error while restoring instance from backup.", e);
            if (orchestrationProperties.postgresClusterRestore().removeFailedToRestoreInstance()) {
                forceDeleteContainerSafe(recoveryContainerId);
                deleteVolumeSafe(volumeName);
            } else {
                log.info("Container ID with failed to restore instance: {} volume name: {} . These resource was not removed according to configuration. Remove them manually.", recoveryContainerId, volumeName);
            }

            if (e instanceof PostgresRestoreException) {
                throw (PostgresRestoreException) e;
            } else {
                throw new PostgresRestoreException("Failed to restore! ", e);
            }
        }
    }

    private void forceDeleteContainerSafe(String containerId) {
        if (containerId != null) {
            try {
                dockerClient.removeContainerCmd(containerId).withForce(true).withRemoveVolumes(true).exec();
            } catch (Exception ex) {
                log.error("Failed to remove unneeded container. Remove it manually. Container id: {}", containerId);
            }
        }
    }

    private void deleteVolumeSafe(String volumeName) {
        if (volumeName != null) {
            try {
                dockerClient.removeVolumeCmd(volumeName).exec();
            } catch (Exception ex) {
                log.error("Failed to remove unneeded volume. Remove it manually. Volume name: {}", volumeName);
            }
        }
    }

    private String instanceIdToContainerId(UUID instanceId) {
        return Optional.ofNullable(persistedProperties.getPostgresNodeInfo(instanceId))
                .map(PostgresPersistedNodeInfo::getAdapterIdentifier)
                .orElse(null);
    }

    private String createVolumeWithPgBaseBackupForStandby(String containerPostfix, Map<String, String> settings) {
        OrchestrationProperties.DockerProperties dockerProperties = orchestrationProperties.docker();

        String volumeName = null, containerId = null;

        try {
            CreateVolumeResponse createVolumeResponse = dockerClient.createVolumeCmd()
                    .withName(dockerUtils.createUniqueObjectName(dockerProperties.postgres().volumeName(), containerPostfix))
                    .exec();

            volumeName = createVolumeResponse.getName();

            CreateContainerResponse tempCreateContainerResponse = dockerClient.createContainerCmd(dockerProperties.postgres().imageTag())
                    .withName(dockerUtils.createUniqueObjectName(dockerProperties.helperObjectName()))
                    .withHostConfig(
                            HostConfig.newHostConfig()
                                    .withBinds(
                                            new Bind(
                                                    volumeName,
                                                    new Volume(DockerConstants.HELP_CONTAINER_BASE_BACKUP_PATH)
                                            )
                                    )
                                    .withNetworkMode(dockerProperties.postgres().networkName())
                    )
                    //we only need Postgres utils like pg_basebackup and don't want to start DB itself
                    .withEntrypoint("sleep", "infinity")
                    .exec();

            containerId = tempCreateContainerResponse.getId();

            dockerClient.startContainerCmd(containerId).exec();

            List<String> settingsLines = new ArrayList<>();
            for (var settingEntry : settings.entrySet()) {
                settingsLines.add(String.format(PostgresConstants.CONF_FILE_LINE_FORMAT, settingEntry.getKey(), settingEntry.getValue()));
            }
            String confFilePath = DockerConstants.HELP_CONTAINER_BASE_BACKUP_PATH + "/" + PostgresqlConfConstants.PG_FACADE_POSTGRESQL_CONF_FILE_NAME;

            String commandToExecute = postgresUtils.getCommandToCreatePgPassFileForPrimary(postgresProperties.users().replication())
                    + " ; " + postgresUtils.createPgBaseBackupCommand(DockerConstants.HELP_CONTAINER_BASE_BACKUP_PATH)
                    + " ; touch " + DockerConstants.HELP_CONTAINER_BASE_BACKUP_PATH + "/standby.signal"
                    + " ; echo \"" + String.join("\n", settingsLines) + "\" > " + confFilePath;

            log.info("Creating backup for standby. This will take some time...");

            AdapterShellCommandExecutionResult commandExecutionResult = executeCmdInContainer(
                    containerId,
                    commandToExecute,
                    List.of(0L),
                    null
            );

            if (!commandExecutionResult.isSuccess()) {
                log.error("Error while creating backup. Message from CMD: {}", commandExecutionResult.getStderr());
                return null;
            }

            log.info("Finished creating backup for standby.");

            forceDeleteContainerSafe(containerId);

            return createVolumeResponse.getName();

        } catch (Exception e) {
            log.error("Error while creating volume with backup.", e);
            forceDeleteContainerSafe(containerId);
            deleteVolumeSafe(volumeName);
            return null;
        }
    }

    private String createEnvValueForRequest(String varName, String value) {
        return varName + "=" + value;
    }

    private CreateContainerCmd getDefaultCreateContainerCmdRequest(String containerNamePostfix) {
        OrchestrationProperties.DockerProperties dockerProperties = orchestrationProperties.docker();

        return dockerClient.createContainerCmd(dockerProperties.postgres().imageTag())
                .withName(dockerUtils.createUniqueObjectName(dockerProperties.postgres().containerName(), containerNamePostfix))
                .withHostConfig(
                        HostConfig.newHostConfig()
                                .withNetworkMode(dockerProperties.postgres().networkName())
                )
                .withEnv(
                        List.of(
                                createEnvValueForRequest(DockerConstants.POSTGRES_ENV_VAR_PASSWORD, postgresProperties.users().superuser().password()),
                                createEnvValueForRequest(DockerConstants.POSTGRES_ENV_VAR_USERNAME, postgresProperties.users().superuser().username()),
                                createEnvValueForRequest(DockerConstants.POSTGRES_ENV_VAR_DB, postgresProperties.users().superuser().database())
                        )
                )
                //TODO bad healthcheck for standby. check using pg_stat_wal_receiver
                .withHealthcheck(
                        new HealthCheck()
                                .withInterval(TimeUnit.MILLISECONDS.toNanos(dockerProperties.postgres().healthcheck().interval()))
                                .withRetries(dockerProperties.postgres().healthcheck().retries())
                                .withStartPeriod(TimeUnit.MILLISECONDS.toNanos(dockerProperties.postgres().healthcheck().startPeriod()))
                                .withTimeout(TimeUnit.MILLISECONDS.toNanos(dockerProperties.postgres().healthcheck().timeout()))
                                .withTest(List.of(DockerConstants.HEALTHCHECK_CMD_SHELL, dockerProperties.postgres().healthcheck().cmdShellCommand()))
                );
    }

    private AdapterShellCommandExecutionResult executeCmdInContainer(String containerId, String shellCommand, List<Long> okExitCodes, String user) {
        try {
            ExecCreateCmd backupExecCreateCmd = dockerClient.execCreateCmd(containerId)
                    .withAttachStdout(true)
                    .withAttachStderr(true)
                    .withCmd("/bin/sh", "-c", shellCommand);

            if (StringUtils.isNotEmpty(user)) {
                backupExecCreateCmd.withUser(user);
            }

            ExecCreateCmdResponse backupExecCreateResponse = backupExecCreateCmd.exec();

            ByteArrayOutputStream stdOut = new ByteArrayOutputStream();
            ByteArrayOutputStream stdErr = new ByteArrayOutputStream();

            dockerClient.execStartCmd(backupExecCreateResponse.getId())
                    .withDetach(false)
                    .exec(new ExecStartResultCallback(stdOut, stdErr))
                    .awaitCompletion();

            InspectExecResponse inspectBackupExecResponse = dockerClient.inspectExecCmd(backupExecCreateResponse.getId()).exec();

            boolean success = false;

            if (CollectionUtils.isNotEmpty(okExitCodes)) {
                success = okExitCodes.contains(inspectBackupExecResponse.getExitCodeLong());
            } else {
                success = true;
            }

            AdapterShellCommandExecutionResult ret = AdapterShellCommandExecutionResult
                    .builder()
                    .success(success)
                    .stderr(stdErr.toString())
                    .stdout(stdOut.toString())
                    .build();

            stdErr.close();
            stdOut.close();

            return ret;
        } catch (Exception e) {
            log.error("Error while executing shell command in Docker container ", e);
            return AdapterShellCommandExecutionResult
                    .builder()
                    .success(false)
                    .build();
        }
    }
}
