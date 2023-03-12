package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.BackupCreationException;
import com.lantromipis.orchestration.exception.PostgresRestoreException;

import java.util.UUID;

public interface PostgresArchiver {

    /**
     * Minimal initialization for archiver just to be able to restore from backup.
     */
    void initializeForStartupRecovery();

    /**
     * Initialize archiver. Must be called after orchestrator initialization completed.
     */
    void initialize();

    boolean doesAnyBackupExist();

    void createAndUploadBackup() throws BackupCreationException;

    UUID restorePostgresToLatestVersionFromArchive() throws PostgresRestoreException;
}
