package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.BackupCreationException;

public interface PostgresArchiver {
    void initialize();

    void createAndUploadBackup() throws BackupCreationException;
}
