package com.lantromipis.adapter.api;

import com.lantromipis.exception.PostgresInstanceCreationException;
import com.lantromipis.exception.PostgresInstanceStartException;
import com.lantromipis.model.PostgresInstanceCreationRequest;
import com.lantromipis.model.PostgresInstanceInfo;

import java.util.List;
import java.util.UUID;

public interface OrchestrationAdapter {
    void initialize();

    PostgresInstanceInfo createNewPostgresInstance(PostgresInstanceCreationRequest request) throws PostgresInstanceCreationException;

    PostgresInstanceInfo tryToStartPostgresInstance(UUID instanceId) throws PostgresInstanceStartException;

    List<PostgresInstanceInfo> getAvailablePostgresInstances();
}
