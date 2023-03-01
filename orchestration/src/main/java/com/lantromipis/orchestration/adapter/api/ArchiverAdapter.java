package com.lantromipis.orchestration.adapter.api;

import com.lantromipis.orchestration.exception.BackupCreationException;

public interface ArchiverAdapter {
    void initialize();

    void createNewBackup() throws BackupCreationException;
}
