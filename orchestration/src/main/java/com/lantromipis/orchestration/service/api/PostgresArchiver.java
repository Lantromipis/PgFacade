package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.BackupCreationException;
import com.lantromipis.orchestration.exception.PostgresRestoreException;

import java.time.Instant;
import java.util.List;

public interface PostgresArchiver {

    /**
     * Minimal initialization for archiver just to be able to restore from backup.
     */
    void initializeForRecovery();

    /**
     * Initialize archiver. Must be called after orchestrator initialization completed.
     */
    void initialize();

    List<Instant> getBackupInstants();

    void createAndUploadBackup() throws BackupCreationException;

    String restorePostgresToLatestVersionFromArchive() throws PostgresRestoreException;
}
