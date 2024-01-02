package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.event.SwitchoverCompletedEvent;
import com.lantromipis.configuration.event.SwitchoverStartedEvent;
import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.properties.predefined.ArchivingProperties;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.configuration.properties.runtime.PostgresSettingsRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.ArchiverStorageAdapter;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.BackupCreationException;
import com.lantromipis.orchestration.model.BaseBackupCreationResult;
import com.lantromipis.orchestration.model.raft.PostgresPersistedArchiverInfo;
import com.lantromipis.orchestration.service.api.PostgresArchiver;
import com.lantromipis.orchestration.service.api.PostgresContinuousArchivingService;
import com.lantromipis.orchestration.util.RaftFunctionalityCombinator;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@ApplicationScoped
public class PostgresArchiverImpl implements PostgresArchiver {

    @Inject
    Instance<ArchiverStorageAdapter> archiverAdapter;

    @Inject
    Instance<PlatformAdapter> platformAdapter;

    @Inject
    ArchivingProperties archivingProperties;
    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    @Inject
    RaftFunctionalityCombinator raftFunctionalityCombinator;

    @Inject
    PostgresContinuousArchivingService postgresContinuousArchivingService;

    @Inject
    PostgresSettingsRuntimeProperties postgresSettingsRuntimeProperties;

    private boolean archiverActiveAndIsLeader = false;
    private boolean archiverInitialized = false;

    private final AtomicBoolean backupCreationInProgress = new AtomicBoolean(false);
    private final AtomicBoolean switchoverInProgress = new AtomicBoolean(false);
    private final AtomicInteger streamingActiveUnsuccessfulRetries = new AtomicInteger(0);

    @Override
    public void initialize() {
        if (!archiverInitialized) {
            archiverAdapter.get().initializeAndValidate();
            archiverInitialized = true;
        }
    }

    @Override
    public void stop() {
        archiverActiveAndIsLeader = false;
        postgresContinuousArchivingService.stopContinuousArchiving();
        log.info("Postgres archiver stopped.");
    }

    @Override
    public void startArchiving() {
        if (!archivingProperties.enabled()) {
            log.info("Archiving is disabled. Consider enabling it to perform continuous archiving and Point-in-Time recover.");
            return;
        }

        log.info("Initializing archiver.");

        if (!archiverInitialized) {
            initialize();
        }

        if (PgFacadeRaftRole.FOLLOWER.equals(pgFacadeRuntimeProperties.getRaftRole())) {
            log.info("Not starting archiving because this PgFacade instance is not current raft leader.");
            archiverActiveAndIsLeader = false;
            return;
        }

        checkIfArchiverInfoExistsAndIsCorrect();

        postgresContinuousArchivingService.startContinuousArchiving(false);

        checkBackupsState();

        archiverActiveAndIsLeader = true;

        log.info("Archiver initialization completed.");
    }

    @Override
    public List<Instant> getBackupInstants() {
        return archiverAdapter.get().getBackupInstants();
    }

    @Override
    public void createAndUploadBackup() throws BackupCreationException {
        log.info("Started creating basebackup for archiving.");
        try {
            raftFunctionalityCombinator.testIfAbleToCommitToRaft();
        } catch (Exception e) {
            throw new BackupCreationException("Failed to create backup due to Raft error!");
        }

        Instant instant = Instant.now();

        BaseBackupCreationResult baseBackupCreationResult = platformAdapter.get().createBaseBackupAndGetAsStream();
        if (!baseBackupCreationResult.isSuccess()) {
            throw new BackupCreationException("Failed to get basebackup for archiving.");
        }

        try {
            log.info("Uploading new basebackup for archiving.");
            raftFunctionalityCombinator.testIfAbleToCommitToRaft();
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
        if (archiverActiveAndIsLeader && backupCreationInProgress.compareAndSet(false, true)) {
            try {
                List<Instant> backups = archiverAdapter.get().getBackupInstants();

                // will never remove backup if it is the last one
                if (archivingProperties.basebackup().cleanUp().removeOld() && backups != null && backups.size() > 1) {
                    // if not leader -> do not remove backups
                    raftFunctionalityCombinator.testIfAbleToCommitToRaft();

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
                backupCreationInProgress.set(false);
            }
        }
    }

    @Scheduled(every = "${pg-facade.archiving.wal-streaming.fault-tolerance.streaming-active-check-interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void checkWalStreamingActive() {
        if (!archiverActiveAndIsLeader || switchoverInProgress.get()) {
            return;
        }

        if (postgresContinuousArchivingService.isContinuousArchivingActive()) {
            streamingActiveUnsuccessfulRetries.set(0);
            return;
        }

        // try to restart continuous archiving
        boolean success = postgresContinuousArchivingService.startContinuousArchiving(false);

        if (success) {
            return;
        }

        int unsuccessfulRetries = streamingActiveUnsuccessfulRetries.incrementAndGet();
        if (unsuccessfulRetries < archivingProperties.walStreaming().faultTolerance().maxUnsuccessfulRetriesBeforeForceRestart()) {
            log.info("Failed to restart continuous WAL archiving from last known LSN {} times. Will retry up to {} times",
                    unsuccessfulRetries,
                    archivingProperties.walStreaming().faultTolerance().maxUnsuccessfulRetriesBeforeForceRestart()
            );
            return;
        }

        log.error("Reached maximum number of attempts to restart continuous WAL archiving using last known LSN. " +
                "Maybe required WAL has already been recycled. Will try to restart streaming using latest server LSN..."
        );
        boolean restartedForLatestLsn = postgresContinuousArchivingService.startContinuousArchiving(true);

        if (!restartedForLatestLsn) {
            log.error("Failed to restart continuous WAL archiving using last known LSN!");
            return;
        }

        streamingActiveUnsuccessfulRetries.set(0);
        log.info("Successfully restarted continuous WAL archiving using latest available LSN.");

        if (archivingProperties.walStreaming().faultTolerance().createNewBackupInCaseOfForceRetry()) {
            log.info("Force creating new backup because continuous WAL archiving was restarted for last known LSN");
            // TODO async + guarantee that backup will be created
            try {
                createAndUploadBackup();
            } catch (Exception e) {
                log.error("Failed to upload backup!");
            }
        } else {
            log.warn("Restarted continuous WAL archiving using last known LSN, but configuration prohibits to create new backup. Data might be lost!");
        }
    }

    public void listenToSwitchoverStartedEvent(@Observes SwitchoverStartedEvent switchoverStartedEvent) {
        log.info("Stopping continuous WAL archiving due to failover/switchover...");
        switchoverInProgress.set(true);
        postgresContinuousArchivingService.stopContinuousArchiving();
    }

    public void listenToSwitchoverStartedEvent(@Observes SwitchoverCompletedEvent switchoverCompletedEvent) {
        log.info("Restarting continuous WAL archiving after failover/switchover...");
        postgresContinuousArchivingService.startContinuousArchiving(false);
        switchoverInProgress.set(false);
    }

    private void checkIfArchiverInfoExistsAndIsCorrect() {
        PostgresPersistedArchiverInfo postgresPersistedArchiverInfo = raftFunctionalityCombinator.getArchiveInfo();

        if (postgresPersistedArchiverInfo == null) {
            postgresPersistedArchiverInfo = new PostgresPersistedArchiverInfo();
            postgresPersistedArchiverInfo.setWalSegmentSizeInBytes(postgresPersistedArchiverInfo.getWalSegmentSizeInBytes());
            raftFunctionalityCombinator.saveArchiverInfoInRaft(postgresPersistedArchiverInfo);
            return;
        }

        if (postgresPersistedArchiverInfo.getWalSegmentSizeInBytes() == 0) {
            postgresPersistedArchiverInfo.setWalSegmentSizeInBytes(postgresSettingsRuntimeProperties.getWalSegmentSizeInBytes());
            raftFunctionalityCombinator.saveArchiverInfoInRaft(postgresPersistedArchiverInfo);
        }
    }
}
