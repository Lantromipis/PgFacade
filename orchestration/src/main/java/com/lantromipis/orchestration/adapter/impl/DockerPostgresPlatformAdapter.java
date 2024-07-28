package com.lantromipis.orchestration.adapter.impl;

import com.github.dockerjava.api.command.*;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.exception.NotModifiedException;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.orchestration.adapter.api.PostgresPlatformAdapter;
import com.lantromipis.orchestration.constant.CommandsConstants;
import com.lantromipis.orchestration.constant.DockerConstants;
import com.lantromipis.orchestration.exception.PlatformAdapterNotFoundException;
import com.lantromipis.orchestration.exception.PlatformAdapterOperationExecutionException;
import com.lantromipis.orchestration.exception.PostgresRestoreException;
import com.lantromipis.orchestration.model.AdapterShellCommandExecutionResult;
import com.lantromipis.orchestration.model.BaseBackupCreationResult;
import com.lantromipis.orchestration.model.PostgresAdapterInstanceInfo;
import com.lantromipis.orchestration.model.PostgresInstanceCreationRequest;
import com.lantromipis.orchestration.util.DockerUtils;
import com.lantromipis.orchestration.util.PgFacadeIOUtils;
import com.lantromipis.orchestration.util.PostgresUtils;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.input.ObservableInputStream;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Matcher;

@Slf4j
@ApplicationScoped
@LookupIfProperty(name = "pg-facade.orchestration.adapter", stringValue = "docker")
public class DockerPostgresPlatformAdapter implements PostgresPlatformAdapter {

    @Inject
    OrchestrationProperties orchestrationProperties;

    @Inject
    PostgresProperties postgresProperties;

    @Inject
    DockerUtils dockerUtils;

    @Inject
    PostgresUtils postgresUtils;

    @Inject
    PgFacadeIOUtils pgFacadeIOUtils;

    @Inject
    DockerClientManager dockerClientManager;

    @Override
    public String createNewPostgresStandbyInstance(PostgresInstanceCreationRequest request) throws PlatformAdapterOperationExecutionException {
        //used to delete container if it was created but method failed.
        String containerId = null;

        try {
            String containerNamePostfix = request.getFutureInstanceId().toString();

            CreateContainerCmd createContainerCmd = getPostgresDefaultCreateContainerCmdRequest(containerNamePostfix);
            String volumeName = createVolumeWithPgBaseBackupForStandby(containerNamePostfix);

            createContainerCmd.getHostConfig()
                    .withBinds(
                            new Bind(
                                    volumeName,
                                    new Volume(orchestrationProperties.docker().postgres().imagePgData())
                            )
                    )
                    .withMemory(
                            dockerUtils.getMemoryBytesFromString(orchestrationProperties.docker().postgres().resources().memoryLimit())
                    )
                    .withNanoCPUs(
                            dockerUtils.getNanoCpusFromDecimalCpus(orchestrationProperties.docker().postgres().resources().cpuLimit())
                    );

            CreateContainerResponse createResponse = createContainerCmd.exec();
            containerId = createResponse.getId();

            log.info("Created container with stand-by. Ready to start it.");

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
            dockerClientManager.getDockerClient().startContainerCmd(adapterInstanceId).exec();
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
            dockerClientManager.getDockerClient().stopContainerCmd(adapterInstanceId).exec();
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
            dockerClientManager.getDockerClient().restartContainerCmd(adapterInstanceId).exec();
        } catch (NotFoundException notFoundException) {
            throw new PlatformAdapterNotFoundException("Failed to restart Postgres container. Container with ID " + adapterInstanceId + " not found.");
        } catch (Exception e) {
            log.error("Unexpected error while restarting Postgres instance", e);
            throw new PlatformAdapterOperationExecutionException("Failed to restart container. Unexpected error! Container ID: " + adapterInstanceId + " ");
        }
    }

    @Override
    public PostgresAdapterInstanceInfo getPostgresInstanceInfo(String adapterInstanceId) throws PlatformAdapterNotFoundException, PlatformAdapterOperationExecutionException {
        try {
            InspectContainerResponse inspectResponse = dockerClientManager.getDockerClient()
                    .inspectContainerCmd(adapterInstanceId)
                    .exec();

            return PostgresAdapterInstanceInfo
                    .builder()
                    .adapterInstanceId(adapterInstanceId)
                    .instanceAddress(dockerUtils.getContainerAddress(inspectResponse, orchestrationProperties.docker().postgres().networkName()))
                    .instancePort(5432)
                    .isActive(DockerConstants.ContainerState.RUNNING.getValue().equals(inspectResponse.getState().getStatus()))
                    .build();

        } catch (NotFoundException notFoundException) {
            throw new PlatformAdapterNotFoundException("Failed to get info about Postgres container. Container with ID " + adapterInstanceId + " not found.");
        } catch (Exception e) {
            throw new PlatformAdapterOperationExecutionException("Failed to get info about Postgres container. ", e);
        }
    }

    @Override
    public boolean deletePostgresInstance(String adapterInstanceId) {
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
                    log.warn("Failed to remove volume {}", bind.getPath());
                }
            }

            return true;
        } catch (NotFoundException notFoundException) {
            return true;
        } catch (Exception e) {
            log.error("Failed to remove container with ID {}. Remove it manually.", adapterInstanceId, e);
            return false;
        }

    }

    @Override
    public BaseBackupCreationResult createBaseBackupAndGetAsStream() {
        OrchestrationProperties.DockerProperties dockerProperties = orchestrationProperties.docker();

        CreateContainerResponse tempCreateContainerResponse = dockerClientManager.getDockerClient()
                .createContainerCmd(dockerProperties.postgres().imageTag())
                .withName(dockerUtils.createUniqueObjectName(dockerProperties.helperObjectName()))
                .withHostConfig(
                        HostConfig.newHostConfig()
                                .withNetworkMode(dockerProperties.postgres().networkName())
                )
                //we only need Postgres utils like pg_basebackup and don't want to start DB itself
                .withEntrypoint("sleep", "infinity")
                .exec();

        String containerId = tempCreateContainerResponse.getId();
        dockerClientManager.getDockerClient().startContainerCmd(containerId).exec();

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
                dockerClientManager.getDockerClient()
                        .removeContainerCmd(containerId)
                        .withForce(true)
                        .withRemoveVolumes(true)
                        .exec();
                return BaseBackupCreationResult
                        .builder()
                        .success(false)
                        .build();
            }

            ObservableInputStream ret = new ObservableInputStream(
                    dockerClientManager.getDockerClient().copyArchiveFromContainerCmd(
                                    containerId,
                                    DockerConstants.HELP_CONTAINER_BASE_BACKUP_PATH + "/"
                            )
                            .exec()
            );

            ret.add(new ObservableInputStream.Observer() {
                final AtomicBoolean closed = new AtomicBoolean(false);

                @Override
                public void closed() throws IOException {
                    if (closed.compareAndSet(false, true)) {
                        forceDeleteContainerSafe(tempCreateContainerResponse.getId());
                        super.closed();
                    } else {
                        super.closed();
                    }
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
            CreateVolumeResponse createVolumeResponse = dockerClientManager.getDockerClient().createVolumeCmd()
                    .withName(dockerUtils.createUniqueObjectName(dockerProperties.postgres().volumeName(), containerNamePostfix))
                    .exec();

            volumeName = createVolumeResponse.getName();

            CreateContainerResponse tempCreateContainerResponse = dockerClientManager.getDockerClient().createContainerCmd(dockerProperties.postgres().imageTag())
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
                    .withEntrypoint("sh", "-c", "mkdir -p "
                            + DockerConstants.HELP_CONTAINER_RESTORE_PGDATA_PATH + " "
                            + DockerConstants.HELP_CONTAINER_RESTORE_BACKUP_PATH + " "
                            + DockerConstants.HELP_CONTAINER_RESTORE_WAL_PATH + " ; sleep infinity")
                    .exec();

            recoveryContainerId = tempCreateContainerResponse.getId();

            dockerClientManager.getDockerClient().startContainerCmd(recoveryContainerId).exec();

            // copy backup
            try (basebackupTarInputStream) {
                dockerClientManager.getDockerClient().copyArchiveToContainerCmd(recoveryContainerId)
                        .withRemotePath(DockerConstants.HELP_CONTAINER_RESTORE_BACKUP_PATH)
                        .withTarInputStream(basebackupTarInputStream)
                        .exec();
            }

            AdapterShellCommandExecutionResult findPgWalCommandResult = executeCmdInContainer(
                    recoveryContainerId,
                    "find " + DockerConstants.HELP_CONTAINER_RESTORE_BACKUP_PATH + " -type d -name pg_wal",
                    List.of(0L),
                    null
            );
            if (!findPgWalCommandResult.isSuccess() || StringUtils.isEmpty(findPgWalCommandResult.getStdout())) {
                throw new PostgresRestoreException("Recovery failed! Failed to find pg_wal dir in base backup for restore. Is backup valid?");
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
                    dockerClientManager.getDockerClient().copyArchiveToContainerCmd(recoveryContainerId)
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

            dockerClientManager.getDockerClient().stopContainerCmd(recoveryContainerId);
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
                throw new PostgresRestoreException("Failed to restore! " + e.getMessage(), e);
            }
        }
    }

    private void forceDeleteContainerSafe(String containerId) {
        if (containerId != null) {
            try {
                dockerClientManager.getDockerClient().removeContainerCmd(containerId).withForce(true).withRemoveVolumes(true).exec();
            } catch (Exception ex) {
                log.error("Failed to remove unneeded container. Remove it manually. Container id {}", containerId);
            }
        }
    }

    private void deleteVolumeSafe(String volumeName) {
        if (volumeName != null) {
            try {
                dockerClientManager.getDockerClient().removeVolumeCmd(volumeName).exec();
            } catch (Exception ex) {
                log.error("Failed to remove unneeded volume. Remove it manually. Volume name {}", volumeName);
            }
        }
    }

    private String createVolumeWithPgBaseBackupForStandby(String containerPostfix) throws PlatformAdapterOperationExecutionException {
        OrchestrationProperties.DockerProperties dockerProperties = orchestrationProperties.docker();

        String volumeName = null, containerId = null;

        try {
            CreateVolumeResponse createVolumeResponse = dockerClientManager.getDockerClient().createVolumeCmd()
                    .withName(dockerUtils.createUniqueObjectName(dockerProperties.postgres().volumeName(), containerPostfix))
                    .exec();

            volumeName = createVolumeResponse.getName();

            CreateContainerResponse tempCreateContainerResponse = dockerClientManager.getDockerClient().createContainerCmd(dockerProperties.postgres().imageTag())
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

            dockerClientManager.getDockerClient().startContainerCmd(containerId).exec();

            String commandToExecute = postgresUtils.getCommandToCreatePgPassFileForPrimary(postgresProperties.users().replication())
                    + " ; " + postgresUtils.createPgBaseBackupCommand(DockerConstants.HELP_CONTAINER_BASE_BACKUP_PATH)
                    + " ; touch " + DockerConstants.HELP_CONTAINER_BASE_BACKUP_PATH + "/standby.signal";

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

            if (StringUtils.isNotEmpty(commandExecutionResult.getStderr())) {
                log.warn("There were errors during basebackup creation. Stderr: {}", commandExecutionResult.getStderr());
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

    private CreateContainerCmd getPostgresDefaultCreateContainerCmdRequest(String containerNamePostfix) {
        OrchestrationProperties.DockerProperties dockerProperties = orchestrationProperties.docker();

        return dockerClientManager.getDockerClient().createContainerCmd(dockerProperties.postgres().imageTag())
                .withName(dockerUtils.createUniqueObjectName(dockerProperties.postgres().containerName(), containerNamePostfix))
                .withHostConfig(
                        HostConfig.newHostConfig()
                                .withNetworkMode(dockerProperties.postgres().networkName())
                );
    }

    private AdapterShellCommandExecutionResult executeCmdInContainer(String containerId, String shellCommand, List<Long> okExitCodes, String user) {
        try {
            ExecCreateCmd backupExecCreateCmd = dockerClientManager.getDockerClient().execCreateCmd(containerId)
                    .withAttachStdout(true)
                    .withAttachStderr(true)
                    .withCmd("/bin/sh", "-c", shellCommand);

            if (StringUtils.isNotEmpty(user)) {
                backupExecCreateCmd.withUser(user);
            }

            ExecCreateCmdResponse backupExecCreateResponse = backupExecCreateCmd.exec();

            ByteArrayOutputStream stdOut = new ByteArrayOutputStream();
            ByteArrayOutputStream stdErr = new ByteArrayOutputStream();

            dockerClientManager.getDockerClient().execStartCmd(backupExecCreateResponse.getId())
                    .withDetach(false)
                    .exec(new ExecStartResultCallback(stdOut, stdErr))
                    .awaitCompletion();

            InspectExecResponse inspectBackupExecResponse = dockerClientManager.getDockerClient().inspectExecCmd(backupExecCreateResponse.getId()).exec();

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
