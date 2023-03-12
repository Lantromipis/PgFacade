package com.lantromipis.orchestration.adapter.api;

import com.lantromipis.orchestration.exception.PostgresRestoreException;
import com.lantromipis.orchestration.model.AdapterShellCommandExecutionResult;
import com.lantromipis.orchestration.model.BaseBackupCreationResult;
import com.lantromipis.orchestration.model.PostgresInstanceCreationRequest;
import com.lantromipis.orchestration.model.PostgresAdapterInstanceInfo;

import java.io.InputStream;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

/**
 * Interface for adapters which allow PgFacade to work on multiple platforms.
 */
public interface PlatformAdapter {
    void initialize();

    void shutdown();

    UUID createNewPostgresInstance(PostgresInstanceCreationRequest request);

    boolean startPostgresInstance(UUID instanceId);

    boolean stopPostgresInstance(UUID instanceId);

    boolean restartPostgresInstance(UUID instanceId);

    PostgresAdapterInstanceInfo getInstanceInfo(UUID instanceId);

    List<PostgresAdapterInstanceInfo> getAvailablePostgresInstancesInfos();

    boolean deletePostgresInstance(UUID instanceId, boolean force);

    void updateInstancesAfterSwitchover(UUID newPrimaryInstanceId, UUID oldPrimaryInstanceId);

    AdapterShellCommandExecutionResult executeShellCommandForInstance(UUID instanceId, String shellCommand, List<Long> okExitCodes);

    List<String> getRequiredHbaConfLines();

    //TODO Better to implement Postgres replication protocol https://www.postgresql.org/docs/current/protocol-replication.html
    BaseBackupCreationResult createBaseBackupAndGetAsStream();

    /**
     * Restores Postgres primary from backup.
     *
     * @param basebackupTarInputStream   input stream containing contents of pg_basebackup in TAR format
     * @param walFileNames               a list of WAL file names that will be used for backup
     * @param walFileInputStreamFunction function which accepts WAL file name and returns content of WAL file as InputStream.
     * @return uuid of new primary instance.
     * @throws PostgresRestoreException when something went wrong and restore failed
     */
    UUID restorePrimaryFromBackup(InputStream basebackupTarInputStream, List<String> walFileNames, Function<String, InputStream> walFileInputStreamFunction) throws PostgresRestoreException;
}
