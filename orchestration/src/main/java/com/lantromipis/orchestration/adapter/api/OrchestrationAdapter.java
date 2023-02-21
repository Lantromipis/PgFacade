package com.lantromipis.orchestration.adapter.api;

import com.lantromipis.orchestration.model.PostgresInstanceCreationRequest;
import com.lantromipis.orchestration.model.PostgresAdapterInstanceInfo;

import java.util.List;
import java.util.UUID;

public interface OrchestrationAdapter {
    void initialize();

    void shutdown();

    UUID createNewPostgresInstance(PostgresInstanceCreationRequest request);

    boolean startPostgresInstance(UUID instanceId);

    boolean stopPostgresInstance(UUID instanceId);

    PostgresAdapterInstanceInfo getInstanceInfo(UUID instanceId);

    List<PostgresAdapterInstanceInfo> getAvailablePostgresInstancesInfos();

    boolean deletePostgresInstance(UUID instanceId, boolean force);

    void updateInstancesAfterSwitchover(UUID newMasterInstanceId, UUID oldMasterInstanceId);

    List<String> getRequiredHbaConfLines();
}
