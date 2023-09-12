package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.PostgresRestoreException;

public interface PostgresRestorationService {
    /**
     * Restores Postgres primary from backup. Stops archiver before trying to restore!
     *
     * @return Adapter identifier of restored Postgres master. Instance will not be started.
     * @throws PostgresRestoreException if something went wrong during restoration
     */
    String stopArchiverAndRestorePostgresFromBackup() throws PostgresRestoreException;
}
