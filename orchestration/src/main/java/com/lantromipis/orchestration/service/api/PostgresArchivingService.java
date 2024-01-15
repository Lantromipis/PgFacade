package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.BackupCreationException;

import java.time.Instant;
import java.util.List;

public interface PostgresArchivingService {

    /**
     * Initializes archiver
     */
    void initialize();

    /**
     * Stops archiver
     */
    void stop();

    /**
     * Starts continuous archiving. Must be called when primary healthiness achieved.
     */
    void startArchiving();

    List<Instant> getBackupInstants();

    void createAndUploadBackup() throws BackupCreationException;
}
