package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.BackupCreationException;

public interface PostgresArchiver {
    /**
     * Initialize archiver. Must be called after orchestrator initialization completed.
     */
    void initialize();

    void createAndUploadBackup() throws BackupCreationException;
}
