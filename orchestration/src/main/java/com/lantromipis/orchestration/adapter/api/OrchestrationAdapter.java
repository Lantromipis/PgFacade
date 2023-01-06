package com.lantromipis.orchestration.adapter.api;

import com.lantromipis.orchestration.model.PostgresInstanceCreationRequest;
import com.lantromipis.orchestration.model.PostgresInstanceInfo;

import java.util.List;
import java.util.UUID;

public interface OrchestrationAdapter {
    void initialize();

    UUID createNewPostgresInstance(PostgresInstanceCreationRequest request);

    boolean startPostgresInstance(UUID instanceId);

    List<PostgresInstanceInfo> getAvailablePostgresInstancesInfos();

    PostgresInstanceInfo getInstanceInfo(UUID instanceId);

    boolean deletePostgresInstance(UUID instanceId);
}
