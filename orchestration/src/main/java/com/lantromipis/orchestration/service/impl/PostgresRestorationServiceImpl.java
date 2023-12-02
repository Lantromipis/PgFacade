package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.producers.FilesPathsProducer;
import com.lantromipis.orchestration.adapter.api.ArchiverStorageAdapter;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.PostgresRestoreException;
import com.lantromipis.orchestration.model.BaseBackupDownload;
import com.lantromipis.orchestration.service.api.PostgresArchiver;
import com.lantromipis.orchestration.service.api.PostgresRestorationService;
import com.lantromipis.orchestration.util.RaftFunctionalityCombinator;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@ApplicationScoped
public class PostgresRestorationServiceImpl implements PostgresRestorationService {

    @Inject
    RaftFunctionalityCombinator raftFunctionalityCombinator;

    @Inject
    Instance<PlatformAdapter> platformAdapter;

    @Inject
    Instance<ArchiverStorageAdapter> archiverAdapter;

    @Inject
    PostgresArchiver postgresArchiver;

    @Inject
    FilesPathsProducer filesPathsProducer;

    private AtomicBoolean clusterRestoreInProgress = new AtomicBoolean(false);

    @Override
    public String stopArchiverAndRestorePostgresFromBackup() throws PostgresRestoreException {
        BaseBackupDownload baseBackupDownload = null;
        log.info("Started Postgres restore from backup!");
        try {
            if (!clusterRestoreInProgress.compareAndSet(false, true)) {
                throw new PostgresRestoreException("Cluster restore already in progress.");
            }

            raftFunctionalityCombinator.testIfAbleToCommitToRaft();

            List<Instant> instants = archiverAdapter.get().getBackupInstants();
            if (CollectionUtils.isEmpty(instants)) {
                throw new PostgresRestoreException("No backups found. Can not restore Postgres from backup.");
            }

            archiverAdapter.get().initializeAndValidate();
            postgresArchiver.stop();

            Instant lastBackupInstant = instants.stream().sorted().findFirst().get();

            log.info("Latest base backup found was created at {}. Will recover using it.", lastBackupInstant);

            baseBackupDownload = archiverAdapter.get().downloadBaseBackup(lastBackupInstant);
            List<String> walFiles = archiverAdapter.get().getAllWalFileNamesSortedStartingFrom(baseBackupDownload.getFirstWalFile());
            if (!walFiles.contains(baseBackupDownload.getFirstWalFile())) {
                throw new PostgresRestoreException("Can not recover! Can not find first WAL for backup in storage. Required WAL file name: " + baseBackupDownload.getFirstWalFile());
            }

            raftFunctionalityCombinator.testIfAbleToCommitToRaft();

            String instanceId = platformAdapter.get().restorePrimaryFromBackup(
                    baseBackupDownload.getInputStreamWithBackupTar(),
                    walFiles,
                    walFileName -> archiverAdapter.get().downloadWalFile(walFileName).getInputStream()
            );

            try {
                FileUtils.cleanDirectory(new File(filesPathsProducer.getPostgresWalStreamReceiverDirectoryPath()));
            } catch (Exception e) {
                log.error("Failed to clean temp WAL directory!", e);
            }

            log.info("Successfully recovered Postgres!");

            return instanceId;
        } catch (PostgresRestoreException e) {
            throw e;
        } catch (Exception e) {
            throw new PostgresRestoreException("Unexpected error during restoring Postgres from backup", e);
        } finally {
            clusterRestoreInProgress.set(false);
            if (baseBackupDownload != null && baseBackupDownload.getInputStreamWithBackupTar() != null) {
                try {
                    baseBackupDownload.getInputStreamWithBackupTar().close();
                } catch (Exception ignored) {
                }
            }
        }
    }
}
