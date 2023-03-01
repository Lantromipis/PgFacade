package com.lantromipis.orchestration.adapter.api;

import com.lantromipis.orchestration.model.AdapterShellCommandExecutionResult;
import com.lantromipis.orchestration.model.BaseBackupAsInputStream;
import com.lantromipis.orchestration.model.PostgresInstanceCreationRequest;
import com.lantromipis.orchestration.model.PostgresAdapterInstanceInfo;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.UUID;

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

    void updateInstancesAfterSwitchover(UUID newMasterInstanceId, UUID oldMasterInstanceId);

    AdapterShellCommandExecutionResult executeShellCommandForInstance(UUID instanceId, String shellCommand, List<Long> okExitCodes);

    List<String> getRequiredHbaConfLines();

    //TODO temporary solution. Better to implement Postgres replication protocol https://www.postgresql.org/docs/current/protocol-replication.html
    BaseBackupAsInputStream createBaseBackupAndGetAsStream();
}
