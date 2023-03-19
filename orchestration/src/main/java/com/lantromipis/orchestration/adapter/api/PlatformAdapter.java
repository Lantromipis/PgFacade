package com.lantromipis.orchestration.adapter.api;

import com.lantromipis.orchestration.exception.PlatformAdapterNotFoundException;
import com.lantromipis.orchestration.exception.PlatformAdapterOperationExecutionException;
import com.lantromipis.orchestration.exception.PostgresRestoreException;
import com.lantromipis.orchestration.model.AdapterShellCommandExecutionResult;
import com.lantromipis.orchestration.model.BaseBackupCreationResult;
import com.lantromipis.orchestration.model.PostgresInstanceCreationRequest;
import com.lantromipis.orchestration.model.PostgresAdapterInstanceInfo;

import java.io.InputStream;
import java.util.List;
import java.util.function.Function;

/**
 * Interface for adapters which allow PgFacade to work on multiple platforms.
 * The main concept is to use some adapter-specific identifier for any existing instance. For example, Docker adapter is using container ID as such identifier.
 */
public interface PlatformAdapter {
    /**
     * Initialize adapter
     */
    void initialize();

    /**
     * Shutdown adapter and free all used resources.
     */
    void shutdown();

    /**
     * Creates new Postgres instance.
     * For primary, this will be empty new database created with initdb
     * For standby, this will be a basebackup of existing and running Postgres primary.
     *
     * @param request object describing Postgres instance to be created
     * @return adapter identifier of Postgres instance
     * @throws PlatformAdapterOperationExecutionException if unexpected error occurred
     */
    String createNewPostgresInstance(PostgresInstanceCreationRequest request) throws PlatformAdapterOperationExecutionException;

    /**
     * Start existing Postgres instance
     *
     * @param adapterInstanceId adapter identifier of existing Postgres instance
     * @return true if successfully started or instance is already running. False if failed to start.
     * @throws PlatformAdapterNotFoundException if there is no instance with provided identifier
     */
    boolean startPostgresInstance(String adapterInstanceId) throws PlatformAdapterNotFoundException;

    /**
     * Stop running Postgres instance
     *
     * @param adapterInstanceId adapter identifier of existing Postgres instance
     * @return true is successfully stopped instance or instance is already stopped. False if failed to stop.
     * @throws PlatformAdapterNotFoundException if there is no instance with provided identifier
     */
    boolean stopPostgresInstance(String adapterInstanceId) throws PlatformAdapterNotFoundException;

    /**
     * Restart Postgres instance.
     *
     * @param adapterInstanceId adapter identifier of existing Postgres instance
     * @return true if successfully restarted Postgres instance. False if failed to restart.
     * @throws PlatformAdapterNotFoundException           if there is no instance with provided identifier
     * @throws PlatformAdapterOperationExecutionException if unexpected error occurred
     */
    void restartPostgresInstance(String adapterInstanceId) throws PlatformAdapterNotFoundException, PlatformAdapterOperationExecutionException;

    /**
     * Retrieve info about running Postgres instance
     *
     * @param adapterInstanceId adapter identifier of existing Postgres instance
     * @return object describing Postgres instance
     * @throws PlatformAdapterNotFoundException           if there is no instance with provided identifier
     * @throws PlatformAdapterOperationExecutionException if unexpected error occurred
     */
    PostgresAdapterInstanceInfo getInstanceInfo(String adapterInstanceId) throws PlatformAdapterNotFoundException, PlatformAdapterOperationExecutionException;

    /**
     * Delete Postgres instance
     *
     * @param adapterInstanceId adapter identifier of existing Postgres instance
     * @return true if successfully deleted Postgres instance or if it is already deleted. False if failed to delete.
     */
    boolean deletePostgresInstance(String adapterInstanceId);

    /**
     * Executes shell command for Postgres instance
     *
     * @param adapterInstanceId adapter identifier of existing Postgres instance
     * @param shellCommand      shell command to execute
     * @param okExitCodes       list of exit codes indicating success
     * @return object containing execution result
     */
    AdapterShellCommandExecutionResult executeShellCommandForInstance(String adapterInstanceId, String shellCommand, List<Long> okExitCodes);

    /**
     * Return subnet to which Postgres and PgFacade belong to allow communications between them.
     *
     * @return subnet IP address
     * @throws PlatformAdapterOperationExecutionException
     */
    String getPostgresSubnetIp() throws PlatformAdapterOperationExecutionException;

    //TODO Better to implement Postgres replication protocol https://www.postgresql.org/docs/current/protocol-replication.html
    BaseBackupCreationResult createBaseBackupAndGetAsStream();

    /**
     * Restores Postgres primary from backup.
     *
     * @param basebackupTarInputStream   input stream containing contents of pg_basebackup in TAR format
     * @param walFileNames               a list of WAL file names that will be used for backup
     * @param walFileInputStreamFunction function which accepts WAL file name and returns content of WAL file as InputStream.
     * @return adapter identifier of new Postgres primary
     * @throws PostgresRestoreException when something went wrong and restore failed
     */
    String restorePrimaryFromBackup(InputStream basebackupTarInputStream, List<String> walFileNames, Function<String, InputStream> walFileInputStreamFunction) throws PostgresRestoreException;
}
