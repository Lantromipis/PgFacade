package com.lantromipis.orchestration.adapter.impl;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.*;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.exception.NotModifiedException;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.lantromipis.configuration.properties.constant.PgFacadeConstants;
import com.lantromipis.configuration.properties.constant.PostgresqlConfConstants;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.constant.CommandsConstants;
import com.lantromipis.orchestration.constant.DockerConstants;
import com.lantromipis.orchestration.constant.PostgresConstants;
import com.lantromipis.orchestration.exception.InitializationException;
import com.lantromipis.orchestration.exception.PlatformAdapterNotFoundException;
import com.lantromipis.orchestration.exception.PlatformAdapterOperationExecutionException;
import com.lantromipis.orchestration.exception.PostgresRestoreException;
import com.lantromipis.orchestration.mapper.DockerMapper;
import com.lantromipis.orchestration.model.*;
import com.lantromipis.orchestration.util.DockerUtils;
import com.lantromipis.orchestration.util.PgFacadeIOUtils;
import com.lantromipis.orchestration.util.PostgresUtils;
import io.quarkus.arc.lookup.LookupIfProperty;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.input.ObservableInputStream;
import org.apache.commons.lang3.StringUtils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

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
    PgFacadeIOUtils pgFacadeIOUtils;

    private DockerClient dockerClient;

    public void initializeAndValidate() {
        log.info("Docker is selected as platform adapter.");

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

            // validate PgFacade network exist
            try {
                dockerClient.inspectNetworkCmd()
                        .withNetworkId(orchestrationProperties.docker().pgFacade().networkName())
                        .exec();
            } catch (NotFoundException e) {
                throw new InitializationException("Docker network for PgFacade not found. Expected network name: '" + orchestrationProperties.docker().postgres().networkName() + "'. Create this network or/and change PgFacade configuration.");
            }
        } catch (InitializationException e) {
            throw e;
        } catch (Exception e) {
            throw new InitializationException("Failed to initialize Docker platform adapter! Unexpected error ", e);
        }

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
    public String createNewPostgresInstance(PostgresInstanceCreationRequest request) throws PlatformAdapterOperationExecutionException {
        //used to delete container if it was created but method failed.
        String containerId = null;

        try {
            String containerNamePostfix = request.getFutureInstanceId().toString();

            CreateContainerCmd createContainerCmd = getPostgresDefaultCreateContainerCmdRequest(containerNamePostfix);

            // for primary will create empty DB.
            if (!request.isPrimary()) {
                Map<String, String> postgresSettings = new HashMap<>(request.getSettings());
                postgresSettings.put(PostgresConstants.PRIMARY_CONN_INFO_SETTING_NAME, postgresUtils.getPrimaryConnInfoSetting());

                String volumeName = createVolumeWithPgBaseBackupForStandby(containerNamePostfix, postgresSettings);

                createContainerCmd.getHostConfig()
                        .withBinds(
                                new Bind(
                                        volumeName,
                                        new Volume(orchestrationProperties.docker().postgres().imagePgData())
                                )
                        );
            } else {
                createContainerCmd
                        .withEnv(
                                List.of(
                                        createEnvValueForRequest(DockerConstants.POSTGRES_ENV_VAR_PASSWORD, postgresProperties.users().superuser().password()),
                                        createEnvValueForRequest(DockerConstants.POSTGRES_ENV_VAR_USERNAME, postgresProperties.users().superuser().username()),
                                        createEnvValueForRequest(DockerConstants.POSTGRES_ENV_VAR_DB, postgresProperties.users().superuser().database())
                                )
                        );
            }

            CreateContainerResponse createResponse = createContainerCmd.exec();
            containerId = createResponse.getId();

            if (request.isPrimary()) {
                log.info("Created container with primary. Ready to start it.");
            } else {
                log.info("Created container with stand-by. Ready to start it.");
            }

            return containerId;

        } catch (PlatformAdapterOperationExecutionException e) {
            forceDeleteContainerSafe(containerId);
            throw e;
        } catch (Exception e) {
            forceDeleteContainerSafe(containerId);
            throw new PlatformAdapterOperationExecutionException("Unexpected error while creating new container for new Postgres instance ", e);
        }
    }

    @Override
    public boolean startPostgresInstance(String adapterInstanceId) throws PlatformAdapterNotFoundException {
        if (adapterInstanceId == null) {
            throw new PlatformAdapterNotFoundException("Can not start Postgres container because container ID is null.");
        }

        try {
            dockerClient.startContainerCmd(adapterInstanceId).exec();
            return true;
        } catch (NotFoundException notFoundException) {
            throw new PlatformAdapterNotFoundException("Failed to start Postgres container. Container with ID " + adapterInstanceId + " not found.");
        } catch (NotModifiedException notModifiedException) {
            return true;
        } catch (Exception e) {
            log.error("Unexpected error while starting Postgres container {}", adapterInstanceId, e);
            return false;
        }
    }

    @Override
    public boolean stopPostgresInstance(String adapterInstanceId) throws PlatformAdapterNotFoundException {
        if (adapterInstanceId == null) {
            throw new PlatformAdapterNotFoundException("Can not stop Postgres container because container ID is null.");
        }

        try {
            dockerClient.stopContainerCmd(adapterInstanceId).exec();
            return true;
        } catch (NotFoundException notFoundException) {
            throw new PlatformAdapterNotFoundException("Failed to stop Postgres container. Container with ID " + adapterInstanceId + " not found.");
        } catch (NotModifiedException notModifiedException) {
            return true;
        } catch (Exception e) {
            log.error("Unexpected error while stopping Postgres container {}", adapterInstanceId, e);
            return false;
        }
    }

    @Override
    public void restartPostgresInstance(String adapterInstanceId) throws PlatformAdapterNotFoundException, PlatformAdapterOperationExecutionException {
        if (adapterInstanceId == null) {
            throw new PlatformAdapterNotFoundException("Can not restart Postgres container because container ID is null.");
        }

        try {
            dockerClient.restartContainerCmd(adapterInstanceId).exec();
        } catch (NotFoundException notFoundException) {
            throw new PlatformAdapterNotFoundException("Failed to restart Postgres container. Container with ID " + adapterInstanceId + " not found.");
        } catch (Exception e) {
            log.error("Unexpected error while restarting Postgres instance", e);
            throw new PlatformAdapterOperationExecutionException("Failed to restart container. Unexpected error! Container ID: " + adapterInstanceId + " ");
        }
    }

    @Override
    public PostgresAdapterInstanceInfo getInstanceInfo(String adapterInstanceId) throws PlatformAdapterNotFoundException, PlatformAdapterOperationExecutionException {
        try {
            InspectContainerResponse inspectResponse = dockerClient.inspectContainerCmd(adapterInstanceId).exec();

            return PostgresAdapterInstanceInfo
                    .builder()
                    .adapterInstanceId(adapterInstanceId)
                    .instanceAddress(dockerUtils.getContainerAddress(inspectResponse))
                    .instancePort(5432)
                    .isActive(DockerConstants.ContainerState.RUNNING.getValue().equals(inspectResponse.getState().getStatus()))
                    .health(dockerMapper.toInstanceHealth(inspectResponse))
                    .build();

        } catch (NotFoundException notFoundException) {
            throw new PlatformAdapterNotFoundException("Failed to get info about Postgres container. Container with ID " + adapterInstanceId + " not found.");
        } catch (Exception e) {
            throw new PlatformAdapterOperationExecutionException("Failed to get info about Postgres container. ", e);
        }
    }

    @Override
    public boolean deleteInstance(String adapterInstanceId) {
        if (adapterInstanceId == null) {
            return true;
        }

        try {
            InspectContainerResponse inspectContainerResponse = dockerClient.inspectContainerCmd(adapterInstanceId).exec();

            try {
                dockerClient.stopContainerCmd(adapterInstanceId).exec();
            } catch (Exception ignored) {
            }

            dockerClient.removeContainerCmd(adapterInstanceId).withForce(true).withRemoveVolumes(true).exec();

            for (var bind : inspectContainerResponse.getHostConfig().getBinds()) {
                try {
                    dockerClient.removeVolumeCmd(bind.getPath()).exec();
                } catch (Exception ignored) {
                    log.warn("Failed to remove volume of delete Postgres instance {}", bind.getPath());
                }
            }

            return true;
        } catch (NotFoundException notFoundException) {
            return true;
        } catch (Exception e) {
            log.error("Failed to remove Postgres container with ID {}. Remove it manually.", adapterInstanceId, e);
            return false;
        }

    }

    @Override
    public AdapterShellCommandExecutionResult executeShellCommandForInstance(String adapterInstanceId, String shellCommand, List<Long> okExitCodes) {
        if (adapterInstanceId == null) {
            return AdapterShellCommandExecutionResult
                    .builder()
                    .success(false)
                    .build();
        }

        return executeCmdInContainer(adapterInstanceId, shellCommand, okExitCodes, null);
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
    public String restorePrimaryFromBackup(InputStream basebackupTarInputStream, List<String> walFileNames, Function<String, InputStream> walFileInputStreamFunction) throws PostgresRestoreException {
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

            CreateContainerCmd createContainerCmd = getPostgresDefaultCreateContainerCmdRequest(UUID.randomUUID().toString());

            createContainerCmd.getHostConfig()
                    .withBinds(
                            new Bind(
                                    volumeName,
                                    new Volume(orchestrationProperties.docker().postgres().imagePgData())
                            )
                    );

            CreateContainerResponse createContainerResponse = createContainerCmd.exec();

            return createContainerResponse.getId();
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

    public String getPostgresSubnetIp() {
        Network pgFacadePostgresNetwork;

        try {
            pgFacadePostgresNetwork = dockerClient.inspectNetworkCmd()
                    .withNetworkId(orchestrationProperties.docker().pgFacade().networkName())
                    .exec();
        } catch (Exception e) {
            throw new PlatformAdapterOperationExecutionException("Docker error. Can not find Postgres network. ", e);
        }

        if (CollectionUtils.isEmpty(pgFacadePostgresNetwork.getIpam().getConfig()) || pgFacadePostgresNetwork.getIpam().getConfig().size() > 1) {
            throw new PlatformAdapterOperationExecutionException("Docker error. Found no or several IPAM config for Postgres network. Network must have exactly one IPAM config with subnet in it.");
        }

        return pgFacadePostgresNetwork.getIpam().getConfig().get(0).getSubnet();
    }

    @Override
    public List<PgFacadeRaftNodeInfo> getActiveRaftNodeInfos() throws PlatformAdapterOperationExecutionException {
        Network pgFacadeInternalNetwork;
        try {
            pgFacadeInternalNetwork = dockerClient.inspectNetworkCmd()
                    .withNetworkId(orchestrationProperties.docker().pgFacade().networkName())
                    .exec();
        } catch (Exception e) {
            throw new PlatformAdapterOperationExecutionException("Docker error. Can not find PgFacade internal network. ", e);
        }

        try {
            return pgFacadeInternalNetwork.getContainers()
                    .entrySet()
                    .stream()
                    .map(configEntry -> dockerClient.inspectContainerCmd(configEntry.getKey()).exec())
                    .map(this::inspectContainerResponseToPgFacadeRaftNodeInfo)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new PlatformAdapterOperationExecutionException("Failed to get Raft nodes info. ", e);
        }
    }

    @Override
    public PgFacadeRaftNodeInfo getSelfRaftNodeInfo() throws PlatformAdapterOperationExecutionException {
        String hostname = System.getenv(DockerConstants.DOCKER_ENV_VAR_HOSTNAME);

        if (hostname == null) {
            throw new PlatformAdapterOperationExecutionException("Docker error. PgFacade container has no HOSTNAME env var. Container with PgFacade must have it, and this env var must contain Docker container ID start symbols.");
        }

        Network pgFacadeInternalNetwork;
        try {
            pgFacadeInternalNetwork = dockerClient.inspectNetworkCmd()
                    .withNetworkId(orchestrationProperties.docker().pgFacade().networkName())
                    .exec();
        } catch (Exception e) {
            throw new PlatformAdapterOperationExecutionException("Docker error. Can not find PgFacade internal network. ", e);
        }

        return pgFacadeInternalNetwork.getContainers()
                .entrySet()
                .stream()
                .filter(configEntry -> configEntry.getKey().contains(hostname))
                .findFirst()
                .map(configEntry -> dockerClient.inspectContainerCmd(configEntry.getKey()).exec())
                .map(this::inspectContainerResponseToPgFacadeRaftNodeInfo)
                .orElseThrow(() -> new PlatformAdapterOperationExecutionException("Can not define self IP address. Does container have HOSTNAME env var configured properly AND is connected to PgFacade network? This env var must contain Docker container ID start symbols."));
    }

    @Override
    public PgFacadeRaftNodeInfo createAndStartNewPgFacadeInstance() throws PlatformAdapterOperationExecutionException {
        PgFacadeRaftNodeInfo self = getSelfRaftNodeInfo();
        InspectContainerResponse inspectSelfResponse = dockerClient.inspectContainerCmd(self.getPlatformAdapterIdentifier()).exec();

        UUID instanceId = UUID.randomUUID();

        CreateContainerCmd createContainerCmd = dockerClient.createContainerCmd(inspectSelfResponse.getImageId())
                .withName(dockerUtils.createUniqueObjectName(orchestrationProperties.docker().pgFacade().containerName(), instanceId.toString()));

        if (inspectSelfResponse.getHostConfig().getBinds() != null) {
            for (var bind : inspectSelfResponse.getHostConfig().getBinds()) {
                if (bind.getVolume().getPath().contains(orchestrationProperties.docker().pgFacade().expectedDockerSockFileName())) {
                    createContainerCmd.withHostConfig(
                            HostConfig.newHostConfig()
                                    .withBinds(
                                            new Bind(
                                                    bind.getPath(),
                                                    new Volume(bind.getVolume().getPath())
                                            )
                                    )
                    );
                    break;
                }
            }
        }

        CreateContainerResponse createContainerResponse = createContainerCmd.exec();

        for (var selfNetwork : inspectSelfResponse.getNetworkSettings().getNetworks().keySet()) {
            dockerClient.connectToNetworkCmd()
                    .withContainerId(createContainerResponse.getId())
                    .withNetworkId(selfNetwork)
                    .exec();
        }

        dockerClient.startContainerCmd(createContainerResponse.getId()).exec();
        InspectContainerResponse inspectNewContainerResponse = dockerClient.inspectContainerCmd(createContainerResponse.getId()).exec();

        return inspectContainerResponseToPgFacadeRaftNodeInfo(inspectNewContainerResponse);
    }

    private PgFacadeRaftNodeInfo inspectContainerResponseToPgFacadeRaftNodeInfo(InspectContainerResponse inspectContainerResponse) {
        return PgFacadeRaftNodeInfo
                .builder()
                .platformAdapterIdentifier(inspectContainerResponse.getId())
                .address(
                        inspectContainerResponse.getNetworkSettings()
                                .getNetworks()
                                .get(orchestrationProperties.docker().pgFacade().networkName())
                                .getIpAddress()
                )
                .createdWhen(Instant.parse(inspectContainerResponse.getCreated()))
                .port(PgFacadeConstants.DOCKER_SPECIFIC_PGFACADE_RAFT_PORT)
                .build();
    }

    private void forceDeleteContainerSafe(String containerId) {
        if (containerId != null) {
            try {
                dockerClient.removeContainerCmd(containerId).withForce(true).withRemoveVolumes(true).exec();
            } catch (Exception ex) {
                log.error("Failed to remove unneeded container. Remove it manually. Container id {}", containerId);
            }
        }
    }

    private void deleteVolumeSafe(String volumeName) {
        if (volumeName != null) {
            try {
                dockerClient.removeVolumeCmd(volumeName).exec();
            } catch (Exception ex) {
                log.error("Failed to remove unneeded volume. Remove it manually. Volume name {}", volumeName);
            }
        }
    }

    private String createVolumeWithPgBaseBackupForStandby(String containerPostfix, Map<String, String> settings) throws PlatformAdapterOperationExecutionException {
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
                throw new PlatformAdapterOperationExecutionException("Error while creating basebackup. Stderr: " + commandExecutionResult.getStderr());
            }

            log.info("Finished creating backup for standby.");

            forceDeleteContainerSafe(containerId);

            return createVolumeResponse.getName();

        } catch (PlatformAdapterOperationExecutionException e) {
            forceDeleteContainerSafe(containerId);
            deleteVolumeSafe(volumeName);
            throw e;
        } catch (Exception e) {
            forceDeleteContainerSafe(containerId);
            deleteVolumeSafe(volumeName);
            throw new PlatformAdapterOperationExecutionException("Error while creating volume with backup.", e);
        }
    }

    private String createEnvValueForRequest(String varName, String value) {
        return varName + "=" + value;
    }

    private CreateContainerCmd getPostgresDefaultCreateContainerCmdRequest(String containerNamePostfix) {
        OrchestrationProperties.DockerProperties dockerProperties = orchestrationProperties.docker();

        return dockerClient.createContainerCmd(dockerProperties.postgres().imageTag())
                .withName(dockerUtils.createUniqueObjectName(dockerProperties.postgres().containerName(), containerNamePostfix))
                .withHostConfig(
                        HostConfig.newHostConfig()
                                .withNetworkMode(dockerProperties.postgres().networkName())
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

            boolean success;

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
