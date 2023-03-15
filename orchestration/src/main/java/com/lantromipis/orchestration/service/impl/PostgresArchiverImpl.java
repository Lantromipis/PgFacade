package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.event.SwitchoverCompletedEvent;
import com.lantromipis.configuration.event.SwitchoverStartedEvent;
import com.lantromipis.configuration.producers.FilesPathsProducer;
import com.lantromipis.configuration.properties.predefined.ArchivingProperties;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.ArchiverStorageAdapter;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.constant.ArchiverConstants;
import com.lantromipis.orchestration.exception.BackupCreationException;
import com.lantromipis.orchestration.exception.PostgresRestoreException;
import com.lantromipis.orchestration.model.ArchiverWalStreamingState;
import com.lantromipis.orchestration.model.BaseBackupCreationResult;
import com.lantromipis.orchestration.model.BaseBackupDownload;
import com.lantromipis.orchestration.model.FailedToUploadWalInfo;
import com.lantromipis.orchestration.service.api.PostgresArchiver;
import com.lantromipis.orchestration.util.PostgresUtils;
import io.quarkus.scheduler.Scheduled;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.nio.file.StandardWatchEventKinds.*;

@Slf4j
@ApplicationScoped
public class PostgresArchiverImpl implements PostgresArchiver {

    @Inject
    Instance<ArchiverStorageAdapter> archiverAdapter;

    @Inject
    Instance<PlatformAdapter> platformAdapter;

    @Inject
    ManagedExecutor managedExecutor;

    @Inject
    ArchivingProperties archivingProperties;

    @Inject
    FilesPathsProducer filesPathsProducer;

    @Inject
    PostgresUtils postgresUtils;

    @Inject
    PostgresProperties postgresProperties;

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    private boolean archiverReady = false;
    private boolean archiverInStartupRecoveryMode = false;

    private AtomicBoolean backupModificationInProgress = new AtomicBoolean(false);
    private AtomicBoolean streamingCheckInProgress = new AtomicBoolean(false);
    private AtomicBoolean uploadFailedWalInProgress = new AtomicBoolean(false);

    private ArchiverWalStreamingState archiverWalStreamingState = new ArchiverWalStreamingState();
    private CountDownLatch switchoverLatch = null;

    private ConcurrentMap<String, FailedToUploadWalInfo> failedToUploadWal = new ConcurrentHashMap<>();

    @Override
    public void initializeForStartupRecovery() {
        archiverAdapter.get().initialize();
        archiverInStartupRecoveryMode = true;
    }

    @Override
    public void initialize() {
        log.info("Initializing archiver.");

        archiverInStartupRecoveryMode = false;

        archiverAdapter.get().initialize();

        File walDir = new File(filesPathsProducer.getPostgresWalStreamUploaderDirectoryPath());

        File[] files = walDir.listFiles();

        if (files != null) {
            for (File file : files) {
                if (!file.getName().endsWith(ArchiverConstants.PARTIAL_WAL_FILE_ENDING)) {
                    Instant timestamp;
                    try {
                        BasicFileAttributes attr = Files.readAttributes(Path.of(file.getAbsolutePath()), BasicFileAttributes.class);
                        timestamp = attr.creationTime().toInstant();
                    } catch (Exception ignored) {
                        timestamp = Instant.now();
                    }
                    failedToUploadWal.put(
                            file.getAbsolutePath(),
                            FailedToUploadWalInfo
                                    .builder()
                                    .absoluteFilePath(file.getAbsolutePath())
                                    .timestamp(timestamp)
                                    .build()
                    );
                }
            }
        }

        startWalArchiving();

        archiverReady = true;

        checkBackupsState();

        log.info("Archiver initialization completed.");
    }

    @Override
    public boolean doesAnyBackupExist() {
        return CollectionUtils.isNotEmpty(archiverAdapter.get().getBackupInstants());
    }

    @Override
    public UUID restorePostgresToLatestVersionFromArchive() throws PostgresRestoreException {
        boolean archiverWasReady = archiverReady;
        try {
            if (!archiverReady && !archiverInStartupRecoveryMode) {
                throw new PostgresRestoreException("Archiver not initialized or is disabled by configuration. Can not restore Postgres from backup.");
            }

            archiverReady = false;
            stopWalArchiving();

            List<Instant> instants = archiverAdapter.get().getBackupInstants();
            if (CollectionUtils.isEmpty(instants)) {
                throw new PostgresRestoreException("No backups found. Can not restore Postgres from backup.");
            }

            Instant lastBackupInstant = instants.stream().sorted().findFirst().get();

            BaseBackupDownload baseBackupDownload = archiverAdapter.get().downloadBaseBackup(lastBackupInstant);
            List<String> walFiles = archiverAdapter.get().getAllWalFileNamesSortedStartingFrom(baseBackupDownload.getFirstWalFile());
            if (!walFiles.contains(baseBackupDownload.getFirstWalFile())) {
                throw new PostgresRestoreException("Can not recover! Can not find first WAL for backup in storage. Required WAL file name: " + baseBackupDownload.getFirstWalFile());
            }

            UUID instanceId = platformAdapter.get().restorePrimaryFromBackup(
                    baseBackupDownload.getInputStreamWithBackupTar(),
                    walFiles,
                    walFileName -> archiverAdapter.get().downloadWalFile(walFileName).getInputStream()
            );

            FileUtils.cleanDirectory(new File(filesPathsProducer.getPostgresWalStreamReceiverDirectoryPath()));

            return instanceId;
        } catch (PostgresRestoreException e) {
            throw e;
        } catch (Exception e) {
            throw new PostgresRestoreException("Unexpected error during restoring Postgres from backup", e);
        } finally {
            if (archiverWasReady) {
                archiverReady = true;
            }
        }
    }

    @Override
    public void createAndUploadBackup() throws BackupCreationException {
        log.info("Started creating basebackup for archiving.");
        Instant instant = Instant.now();

        BaseBackupCreationResult baseBackupCreationResult = platformAdapter.get().createBaseBackupAndGetAsStream();
        if (!baseBackupCreationResult.isSuccess()) {
            throw new BackupCreationException("Failed to get basebackup for archiving.");
        }

        try {
            log.info("Uploading new basebackup for archiving.");
            archiverAdapter.get().uploadBackupTar(
                    baseBackupCreationResult.getStream(),
                    instant,
                    baseBackupCreationResult.getFirstWalFileName()
            );

        } catch (Exception e) {
            throw new BackupCreationException("Failed to upload for archiving. ", e);
        } finally {
            try {
                baseBackupCreationResult.getStream().close();
            } catch (Exception ignored) {
            }
        }

        log.info("Successfully created and uploaded base backup for archiving.");
    }

    @Scheduled(every = "${pg-facade.archiving.basebackup.list-backups-interval}")
    public void checkBackupsState() {
        if (archiverReady && backupModificationInProgress.compareAndSet(false, true)) {
            try {
                List<Instant> backups = archiverAdapter.get().getBackupInstants();

                // will never remove backup if it is the last one
                if (archivingProperties.basebackup().cleanUp().removeOld() && backups != null && backups.size() > 1) {
                    Instant oldestPermittedBackupInstant = Instant.now().minus(archivingProperties.basebackup().cleanUp().keepOldInterval());

                    if (archiverAdapter.get().removeBackupsAndWalOlderThanInstant(oldestPermittedBackupInstant, archivingProperties.basebackup().cleanUp().removeOldWalFilesWhenRemoving()) > 0) {
                        log.info("Removed old backups according to configuration.");
                        backups = archiverAdapter.get().getBackupInstants();
                    }
                }

                boolean createBackup;

                if (CollectionUtils.isNotEmpty(backups)) {
                    Instant newestBackupInstant = backups.stream().max(Comparator.naturalOrder()).orElse(null);
                    if (newestBackupInstant == null) {
                        createBackup = true;
                    } else {
                        createBackup = newestBackupInstant.compareTo(Instant.now().minus(archivingProperties.basebackup().createInterval())) < 0;
                    }
                } else {
                    createBackup = true;
                }

                if (createBackup) {
                    createAndUploadBackup();
                }
            } catch (Exception e) {
                log.info("Error while checking or/and creating backup. ", e);
            } finally {
                backupModificationInProgress.set(false);
            }
        }
    }

    @Scheduled(every = "${pg-facade.archiving.wal-streaming.retry-upload-wal-files-interval}")
    public void retryUploadFailedWalFiles() {
        if (archiverReady && uploadFailedWalInProgress.compareAndSet(false, true)) {
            try {
                failedToUploadWal.values()
                        .forEach(failedWalFileInfo ->
                                managedExecutor.runAsync(() -> uploadWalFileByPath(failedWalFileInfo.getAbsoluteFilePath(), failedWalFileInfo.getTimestamp()))
                        );
            } finally {
                uploadFailedWalInProgress.set(false);
            }
        }
    }

    @Scheduled(every = "${pg-facade.archiving.wal-streaming.fault-tolerance.streaming-active-check-interval}")
    public void checkWalStreamingActive() {
        if (archiverReady && streamingCheckInProgress.compareAndSet(false, true)) {
            try {
                if (archiverWalStreamingState.getWatchServiceActive().get() && archiverWalStreamingState.getProcessActive().get()) {
                    archiverWalStreamingState.getUnsuccessfulRetries().set(0);
                } else {
                    stopWalArchiving();

                    boolean notConenctedToSwitchoverError = true;

                    if ((switchoverLatch != null && switchoverLatch.getCount() > 0) || archiverWalStreamingState.getSwitchoverActionProcessed().get()) {
                        notConenctedToSwitchoverError = false;
                        archiverWalStreamingState.getSwitchoverActionProcessed().set(false);
                        try {
                            switchoverLatch.await();
                            try {
                                File walReceiverDir = new File(filesPathsProducer.getPostgresWalStreamReceiverDirectoryPath());
                                File[] walReceiverFiles = walReceiverDir.listFiles();

                                if (walReceiverFiles != null) {
                                    List<File> allPartialFilesSorted = Arrays.stream(walReceiverFiles)
                                            .filter(file -> file.getName().endsWith(ArchiverConstants.PARTIAL_WAL_FILE_ENDING))
                                            .sorted()
                                            .collect(Collectors.toList());

                                    if (allPartialFilesSorted.size() > 2) {
                                        allPartialFilesSorted.remove(0);
                                        allPartialFilesSorted.forEach(File::delete);
                                    }
                                }

                            } catch (Exception e) {
                                log.error("Failed to clear old '*.partial' files. Check dir {} and remove old '*.partial' files manually to free disk space.", filesPathsProducer.getPostgresWalStreamReceiverDirectoryPath());
                            }
                        } catch (InterruptedException e) {
                            log.error("Failed to activate streaming replication. Will retry later...");
                            Thread.currentThread().interrupt();
                            return;
                        }
                        log.info("Restarting WAL stream due to switchover.");
                    }

                    if (notConenctedToSwitchoverError) {
                        String cause = StringUtils.isEmpty(archiverWalStreamingState.getPgReceiveWalStdErr())
                                ? "unknown"
                                : archiverWalStreamingState.getPgReceiveWalStdErr().replaceAll("\n", "");
                        log.error("Restarting WAL streaming due to error. Cause: '{}'", cause);

                        if (archiverWalStreamingState.getUnsuccessfulRetries().incrementAndGet() > archivingProperties.walStreaming().faultTolerance().maxUnsuccessfulRetriesBeforeForceRestart()) {
                            log.error("Reached limit of max unsuccessful retries to restart WAL streaming! Force restarting WAL streaming!");
                            try {
                                FileUtils.cleanDirectory(new File(filesPathsProducer.getPostgresWalStreamReceiverDirectoryPath()));
                            } catch (Exception ignored) {
                            }

                            archiverWalStreamingState.getUnsuccessfulRetries().set(0);

                            startWalArchiving();

                            if (archivingProperties.walStreaming().faultTolerance().createNewBackupInCaseOfForceRetry()) {
                                log.info("Creating and uploading new backup due to streaming force restart.");
                                managedExecutor.runAsync(this::createAndUploadBackup);
                            }

                            return;
                        }
                    }

                    startWalArchiving();
                }
            } finally {
                streamingCheckInProgress.set(false);
            }
        }
    }

    public void listenToSwitchoverStartedEvent(@Observes SwitchoverStartedEvent switchoverStartedEvent) {
        switchoverLatch = new CountDownLatch(1);
        stopWalArchiving();
        archiverWalStreamingState.getSwitchoverActionProcessed().set(true);
    }

    public void listenToSwitchoverStartedEvent(@Observes SwitchoverCompletedEvent switchoverCompletedEvent) {
        switchoverLatch.countDown();
    }

    private void stopWalArchiving() {
        try {
            if (archiverWalStreamingState.getWalDirWatcherService() != null) {
                archiverWalStreamingState.getWalDirWatcherService().close();
            }
        } catch (Exception ignored) {
        }

        if (archiverWalStreamingState.getPgReceiveWallProcess() != null) {
            archiverWalStreamingState.getPgReceiveWallProcess().destroy();
        }
    }

    private void uploadWalOrHistoryFileByNameAsync(String walFileName, Instant timestamp) {
        managedExecutor.runAsync(() -> {
            String walFilePath = filesPathsProducer.getPostgresWalStreamReceiverDirectoryPath() + "/" + walFileName;

            File completedWalFile = new File(walFilePath);
            File movedWalFile = new File(filesPathsProducer.getPostgresWalStreamUploaderDirectoryPath() + "/" + walFileName);
            try {
                FileUtils.moveFile(completedWalFile, movedWalFile);
                walFilePath = movedWalFile.getAbsolutePath();
            } catch (Exception ignored) {
                // not logged. if this fails, any operation with file will fail
            }

            uploadWalFileByPath(walFilePath, timestamp);
        });
    }

    private void uploadWalFileByPath(String walFileAbsolutePath, Instant timestamp) {
        for (int i = 0; i < archivingProperties.walStreaming().uploadWalRetries(); i++) {
            try {
                File file = new File(walFileAbsolutePath);
                archiverAdapter.get().uploadWalFile(file, timestamp);
                file.delete();
                failedToUploadWal.remove(walFileAbsolutePath);
                return;
            } catch (Exception ignored) {
                // do not log this exception due to stack trace spam
            }
        }
        failedToUploadWal.put(
                walFileAbsolutePath,
                FailedToUploadWalInfo
                        .builder()
                        .absoluteFilePath(walFileAbsolutePath)
                        .timestamp(timestamp)
                        .build()
        );
        log.error("FAILED TO UPLOAD WAL FILE! WILL KEEP IT AND RETRY LATER. MOST LIKELY IT IS STORAGE ISSUE.");
    }

    // true if successfully started
    private void startWalArchiving() {
        log.info("Starting WAL streaming from primary...");
        try {
            // creating .pgpass for pg_receivewal
            ProcessBuilder pgPassCreateProcessBuilder = new ProcessBuilder();
            pgPassCreateProcessBuilder.command("/bin/sh", "-c", postgresUtils.getCommandToCreatePgPassFileForPrimary(postgresProperties.users().replication()));
            Process pgPassCreationProcess = pgPassCreateProcessBuilder.start();
            pgPassCreationProcess.waitFor();

            if (pgPassCreationProcess.exitValue() != 0) {
                log.error("Failed to create .pgpass file in PgFacade container! Streaming replication will not work!");
            }

            // start WatchService to monitor file system changes
            archiverWalStreamingState.setWalDirWatcherService(FileSystems.getDefault().newWatchService());
            Path path = Paths.get(filesPathsProducer.getPostgresWalStreamReceiverDirectoryPath());

            // At the end of WAL file pg_receivewal will rename file (remove .partial from end).
            // This means ENTRY_CREATE event will be fired, containing WAL file name without .partial at the end.
            // Other events not needed
            path.register(archiverWalStreamingState.getWalDirWatcherService(), ENTRY_CREATE);
            archiverWalStreamingState.getWatchServiceActive().set(true);

            managedExecutor.runAsync(() -> {
                boolean poll = true;
                try {
                    while (poll) {
                        WatchKey key = archiverWalStreamingState.getWalDirWatcherService().take();
                        for (WatchEvent<?> event : key.pollEvents()) {
                            String fileName = ((Path) event.context()).getFileName().toString();
                            if (!StringUtils.endsWith(fileName, ArchiverConstants.PARTIAL_WAL_FILE_ENDING) && !StringUtils.endsWith(fileName, ArchiverConstants.TMP_HISTORY_FILE_ENDING)) {
                                uploadWalOrHistoryFileByNameAsync(fileName, Instant.now());
                            }
                        }
                        poll = key.reset();
                    }
                } catch (Exception e) {
                    if (log.isDebugEnabled()) {
                        log.error("Error in WAL files watcher service", e);
                    }
                } finally {
                    archiverWalStreamingState.getWatchServiceActive().set(false);
                }
            });

            // start pg_receivewal
            ProcessBuilder pgReceiveWalProcessBuilder = new ProcessBuilder();
            String walStreamDir = filesPathsProducer.getPostgresWalStreamReceiverDirectoryPath();

            pgReceiveWalProcessBuilder.command("/bin/sh", "-c", postgresUtils.createPgReceiveWalCommand(clusterRuntimeProperties.getPrimaryInstanceInfo().getInstanceId(), walStreamDir));
            archiverWalStreamingState.setPgReceiveWallProcess(pgReceiveWalProcessBuilder.start());
            archiverWalStreamingState.getProcessActive().set(true);
            archiverWalStreamingState.getPgReceiveWallProcess().onExit()
                    .thenAccept(process -> {
                                try {
                                    archiverWalStreamingState.setPgReceiveWalStdErr(IOUtils.toString(process.getErrorStream(), StandardCharsets.UTF_8));
                                } catch (Exception ignored) {
                                }

                                archiverWalStreamingState.getProcessActive().set(false);
                            }
                    );

            log.info("Started streaming WAL from primary.");
        } catch (Exception e) {
            log.error("Error while initiating replication stream for WAL.", e);
        }
    }
}
