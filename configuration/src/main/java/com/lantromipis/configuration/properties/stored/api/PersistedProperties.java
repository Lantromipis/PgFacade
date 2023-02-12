package com.lantromipis.configuration.properties.stored.api;

import com.lantromipis.configuration.exception.PropertyReadException;
import com.lantromipis.configuration.exception.PropertySaveException;
import com.lantromipis.configuration.model.PostgresPersistedNodeInfo;

import java.util.List;

public interface PersistedProperties {
    List<PostgresPersistedNodeInfo> getPostgresNodeInfos() throws PropertyReadException;

    void savePostgresNodeInfo(PostgresPersistedNodeInfo postgresPersistedNodeInfo) throws PropertySaveException;
}
