package com.lantromipis.configuration.properties.stored.api;

import com.lantromipis.configuration.exception.PropertyReadException;
import com.lantromipis.configuration.exception.PropertyModifyException;
import com.lantromipis.configuration.model.PostgresPersistedNodeInfo;

import java.util.List;
import java.util.UUID;

public interface PersistedProperties {
    List<PostgresPersistedNodeInfo> getPostgresNodeInfos() throws PropertyReadException;

    PostgresPersistedNodeInfo getPostgresNodeInfo(UUID instanceId) throws PropertyReadException;

    void savePostgresNodeInfo(PostgresPersistedNodeInfo postgresPersistedNodeInfo) throws PropertyModifyException;

    PostgresPersistedNodeInfo deletePostgresNodeInfo(UUID instanceId) throws PropertyModifyException;
}
