package com.lantromipis.configuration.properties.stored.api;

import com.lantromipis.configuration.exception.PropertyReadException;
import com.lantromipis.configuration.exception.PropertyModificationException;
import com.lantromipis.configuration.model.PostgresPersistedInstanceInfo;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface PostgresPersistedProperties {
    List<PostgresPersistedInstanceInfo> getPostgresNodeInfos() throws PropertyReadException;

    PostgresPersistedInstanceInfo getPostgresNodeInfo(UUID instanceId) throws PropertyReadException;

    void savePostgresNodeInfo(PostgresPersistedInstanceInfo postgresPersistedInstanceInfo) throws PropertyModificationException;

    PostgresPersistedInstanceInfo deletePostgresNodeInfo(UUID instanceId) throws PropertyModificationException;

    void clearPostgresNodesInfos() throws PropertyModificationException;

    Map<String, String> getPostgresSettingInfos() throws PropertyReadException;

    void savePostgresSettingsInfos(Map<String, String> persistedSettingsInfos) throws PropertyModificationException;

    void deletePostgresSettingsInfos(List<String> settingsNames) throws PropertyModificationException;
}
