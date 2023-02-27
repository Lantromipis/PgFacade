package com.lantromipis.configuration.properties.stored.api;

import com.lantromipis.configuration.exception.PropertyReadException;
import com.lantromipis.configuration.exception.PropertyModificationException;
import com.lantromipis.configuration.model.PostgresPersistedNodeInfo;
import com.lantromipis.configuration.model.PostgresPersistedSettingInfo;

import java.util.List;
import java.util.UUID;

public interface PostgresPersistedProperties {
    List<PostgresPersistedNodeInfo> getPostgresNodeInfos() throws PropertyReadException;

    PostgresPersistedNodeInfo getPostgresNodeInfo(UUID instanceId) throws PropertyReadException;

    void savePostgresNodeInfo(PostgresPersistedNodeInfo postgresPersistedNodeInfo) throws PropertyModificationException;

    PostgresPersistedNodeInfo deletePostgresNodeInfo(UUID instanceId) throws PropertyModificationException;

    List<PostgresPersistedSettingInfo> getPostgresSettingInfos() throws PropertyReadException;

    void savePostgresSettingsInfos(List<PostgresPersistedSettingInfo> persistedSettingsInfos) throws PropertyModificationException;

    void deletePostgresSettingsInfos(List<String> settingsNames) throws PropertyModificationException;
}
