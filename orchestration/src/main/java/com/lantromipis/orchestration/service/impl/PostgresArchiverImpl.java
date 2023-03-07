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
import com.lantromipis.orchestration.model.ArchiverWalStreamingState;
import com.lantromipis.orchestration.model.BaseBackupAsInputStream;
import com.lantromipis.orchestration.model.FailedToUploadWalInfo;
import com.lantromipis.orchestration.service.api.PostgresArchiver;
import com.lantromipis.orchestration.util.PostgresUtils;
import io.quarkus.scheduler.Scheduled;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.io.File;
import java.math.BigInteger;
import java.nio.file.*;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private AtomicBoolean backupModificationInProgress = new AtomicBoolean(false);
    private AtomicBoolean walDirCheckInProgress = new AtomicBoolean(false);
    private AtomicBoolean uploadFailedWalInProgress = new AtomicBoolean(false);

    private ArchiverWalStreamingState archiverWalStreamingState = new ArchiverWalStreamingState();
    private CountDownLatch switchoverLatch = null;

    private String lastKnownUploadedWal = null;
    private ConcurrentMap<String, FailedToUploadWalInfo> failedToUploadWal = new ConcurrentHashMap<>();

    @Override
    public void initialize() {
        log.info("Initializing archiver.");

        archiverAdapter.get().initialize();

        File walDir = new File(filesPathsProducer.getPostgresWalStreamInfosDirectoryPath());

        File[] files = walDir.listFiles();

        if (files != null) {
            for (File file : files) {
                if (!file.getName().endsWith(ArchiverConstants.PARTIAL_WAL_FILE_ENDING)) {
                    failedToUploadWal.put(
                            file.getName(),
                            FailedToUploadWalInfo
                                    .builder()
                                    .fileName(file.getName())
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
    public void createAndUploadBackup() throws BackupCreationException {
        log.info("Started creating basebackup for archiving.");
        Instant instant = Instant.now();

        BaseBackupAsInputStream baseBackupAsInputStream = platformAdapter.get().createBaseBackupAndGetAsStream();
        if (!baseBackupAsInputStream.isSuccess()) {
            throw new BackupCreationException("Failed to get basebackup for archiving.");
        }

        try {
            log.info("Uploading new basebackup for archiving.");
            archiverAdapter.get().uploadBackup(
                    baseBackupAsInputStream.getStream(),
                    instant,
                    baseBackupAsInputStream.getFirstWalFileName()
            );

        } catch (Throwable e) {
            throw new BackupCreationException("Failed to upload for archiving. ", e);
        } finally {
            try {
                baseBackupAsInputStream.getStream().close();
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

    @Scheduled(every = "${pg-facade.archiving.wal.wal-dir-clear-interval}")
    public void checkWalDir() {
        if (archiverReady && lastKnownUploadedWal != null && walDirCheckInProgress.compareAndSet(false, true)) {
            try {
                File dir = new File(filesPathsProducer.getPostgresWalStreamInfosDirectoryPath());

                File[] files = dir.listFiles();

                if (files == null) {
                    return;
                }

                String lastWal = lastKnownUploadedWal;

                for (var file : files) {
                    if (StringUtils.endsWith(file.getName(), ArchiverConstants.PARTIAL_WAL_FILE_ENDING)) {
                        String walFilename = StringUtils.removeEnd(file.getName(), ArchiverConstants.PARTIAL_WAL_FILE_ENDING);

                        if (postgresUtils.calculateDifferenceBetweenWalFiles(lastWal, walFilename)
                                .abs()
                                .subtract(ArchiverConstants.PARTIAL_WAL_FILE_REMOVE_DIFF)
                                .compareTo(BigInteger.ZERO) > 0) {
                            file.delete();
                        }
                    }
                }
            } finally {
                walDirCheckInProgress.set(false);
            }
        }
    }

    @Scheduled(every = "${pg-facade.archiving.wal.retry-upload-wal-files-interval}")
    public void retryUploadFailedWalFiles() {
        if (archiverReady && uploadFailedWalInProgress.compareAndSet(false, true)) {
            try {
                failedToUploadWal.values().forEach(failedWalFileInfo -> uploadWalFileAsync(failedWalFileInfo.getFileName()));
            } finally {
                uploadFailedWalInProgress.set(false);
            }
        }
    }

    @Scheduled(every = "${pg-facade.archiving.wal.streaming-active-check-interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void checkWalStreamingActive() {
        if (archiverReady) {
            if (!archiverWalStreamingState.getWatchServiceActive().get() || !archiverWalStreamingState.getProcessActive().get()) {
                stopWalArchiving();

                if (switchoverLatch != null) {
                    try {
                        switchoverLatch.await();
                    } catch (InterruptedException e) {
                        log.error("Failed to activate streaming replication. Will retry later...");
                        return;
                    }
                }

                startWalArchiving();
            }
        }
    }

    public void listenToSwitchoverStartedEvent(@Observes SwitchoverStartedEvent switchoverStartedEvent) {
        switchoverLatch = new CountDownLatch(1);
        stopWalArchiving();
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

    private void uploadWalFileAsync(String walFileName) {
        managedExecutor.runAsync(() -> {
            for (int i = 0; i < archivingProperties.wal().uploadWalRetries(); i++) {
                try {
                    File file = new File(filesPathsProducer.getPostgresWalStreamInfosDirectoryPath() + "/" + walFileName);
                    archiverAdapter.get().uploadWalFile(file);
                    file.delete();
                    failedToUploadWal.remove(walFileName);
                    return;
                } catch (Exception ignored) {
                    //do not log this exception due to stack trace spam
                }
            }
            failedToUploadWal.put(
                    walFileName,
                    FailedToUploadWalInfo
                            .builder()
                            .fileName(walFileName)
                            .build()
            );
            log.error("FAILED TO UPLOAD WAL FILE! WILL KEEP IT AND RETRY LATER. MOST LIKELY IT IS STORAGE ISSUE. CHECK PGFACADE HEALTH!");
        });
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
            Path path = Paths.get(filesPathsProducer.getPostgresWalStreamInfosDirectoryPath());

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
                            if (!StringUtils.endsWith(fileName, ArchiverConstants.PARTIAL_WAL_FILE_ENDING)) {
                                uploadWalFileAsync(fileName);
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
            String walStreamDir = filesPathsProducer.getPostgresWalStreamInfosDirectoryPath();

            pgReceiveWalProcessBuilder.command("/bin/sh", "-c", postgresUtils.createPgReceiveWalCommand(clusterRuntimeProperties.getPrimaryInstanceInfo().getInstanceId(), walStreamDir));
            archiverWalStreamingState.setPgReceiveWallProcess(pgReceiveWalProcessBuilder.start());
            archiverWalStreamingState.getProcessActive().set(true);
            archiverWalStreamingState.getPgReceiveWallProcess().onExit().thenAccept(process -> archiverWalStreamingState.getProcessActive().set(false));

            log.info("Started streaming WAL from primary.");
        } catch (Exception e) {
            log.error("Error while initiating replication stream for WAL.", e);
        }
    }
}
