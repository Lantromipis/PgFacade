package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.BackupCreationException;
import com.lantromipis.orchestration.exception.PostgresRestoreException;

import java.time.Instant;
import java.util.List;

public interface PostgresArchiver {

    /**
     * Initializes archiver
     */
    void initialize();

    /**
     * Starts continuous archiving. Must be called when primary healthiness achieved.
     */
    void startArchiving();

    List<Instant> getBackupInstants();

    void createAndUploadBackup() throws BackupCreationException;

    String restorePostgresToLatestVersionFromArchive() throws PostgresRestoreException;
}
