package com.lantromipis.orchestration.service.api.raft;

import com.lantromipis.configuration.exception.PropertyModificationException;
import com.lantromipis.configuration.exception.PropertyReadException;
import com.lantromipis.orchestration.model.raft.PostgresPersistedArchiveInfo;
import com.lantromipis.orchestration.model.raft.PostgresPersistedInstanceInfo;
import com.lantromipis.pgfacadeprotocol.model.api.SnapshotChunk;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface RaftStorage {

    List<SnapshotChunk> getChunks() throws PropertyReadException;

    void loadChunks(List<SnapshotChunk> chunks) throws PropertyModificationException;

    PostgresPersistedArchiveInfo getArchiveInfo() throws PropertyReadException;

    void saveArchiveInfo(PostgresPersistedArchiveInfo info) throws PropertyModificationException;

    List<PostgresPersistedInstanceInfo> getPostgresNodeInfos() throws PropertyReadException;

    PostgresPersistedInstanceInfo getPostgresNodeInfo(UUID instanceId) throws PropertyReadException;

    void savePostgresNodeInfo(PostgresPersistedInstanceInfo postgresPersistedInstanceInfo) throws PropertyModificationException;

    PostgresPersistedInstanceInfo deletePostgresNodeInfo(UUID instanceId) throws PropertyModificationException;

    void clearPostgresNodesInfos() throws PropertyModificationException;

    Map<String, String> getPostgresSettingInfos() throws PropertyReadException;

    void savePostgresSettingsInfos(Map<String, String> persistedSettingsInfos) throws PropertyModificationException;

    void deletePostgresSettingsInfos(List<String> settingsNames) throws PropertyModificationException;
}
