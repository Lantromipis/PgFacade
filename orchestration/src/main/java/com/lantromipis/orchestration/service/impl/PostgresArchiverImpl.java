package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.properties.predefined.ArchivingProperties;
import com.lantromipis.orchestration.adapter.api.ArchiverStorageAdapter;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.BackupCreationException;
import com.lantromipis.orchestration.model.BaseBackupAsInputStream;
import com.lantromipis.orchestration.service.api.PostgresArchiver;
import io.quarkus.scheduler.Scheduled;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private boolean archiverReady = false;

    private AtomicBoolean backupModificationInProgress = new AtomicBoolean(false);

    @Override
    public void initialize() {
        log.info("Initializing archiver.");

        archiverAdapter.get().initialize();

        archiverReady = true;

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
                    instant
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
            List<Instant> backups = archiverAdapter.get().getBackupInstants();

            // will never remove backup if it is the last one
            if (backups != null && backups.size() > 1) {
                Instant oldestPermittedBackupInstant = Instant.now().minus(archivingProperties.basebackup().keepOldInterval());

                if (archiverAdapter.get().removeBackupsAndWalOlderThanInstant(oldestPermittedBackupInstant) > 0) {
                    log.info("Removed old backups and wal files according to configuration.");
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
                try {
                    createAndUploadBackup();
                } catch (Exception e) {
                    log.info("Error while creating new backup because old one was outdated. ", e);
                } finally {
                    backupModificationInProgress.set(false);
                }
            } else {
                backupModificationInProgress.set(false);
            }
        }
    }
}
