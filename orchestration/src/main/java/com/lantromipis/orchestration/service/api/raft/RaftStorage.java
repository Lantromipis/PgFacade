package com.lantromipis.orchestration.service.api.raft;

import com.lantromipis.configuration.exception.PropertyModificationException;
import com.lantromipis.configuration.exception.PropertyReadException;
import com.lantromipis.orchestration.model.raft.ExternalLoadBalancerRaftInfo;
import com.lantromipis.orchestration.model.raft.PostgresPersistedArchiverInfo;
import com.lantromipis.orchestration.model.raft.PostgresPersistedInstanceInfo;
import com.lantromipis.pgfacadeprotocol.model.api.SnapshotChunk;

import java.util.List;
import java.util.UUID;

public interface RaftStorage {

    List<SnapshotChunk> getChunks() throws PropertyReadException;

    void loadChunks(List<SnapshotChunk> chunks) throws PropertyModificationException;

    PostgresPersistedArchiverInfo getArchiveInfo() throws PropertyReadException;

    void saveArchiveInfo(PostgresPersistedArchiverInfo info) throws PropertyModificationException;

    void deleteArchiveInfo() throws PropertyModificationException;

    List<PostgresPersistedInstanceInfo> getPostgresNodeInfos() throws PropertyReadException;

    PostgresPersistedInstanceInfo getPostgresNodeInfo(UUID instanceId) throws PropertyReadException;

    void savePostgresNodeInfo(PostgresPersistedInstanceInfo postgresPersistedInstanceInfo) throws PropertyModificationException;

    PostgresPersistedInstanceInfo deletePostgresNodeInfo(UUID instanceId) throws PropertyModificationException;

    void clearPostgresNodesInfos() throws PropertyModificationException;

    ExternalLoadBalancerRaftInfo getPgFacadeLoadBalancerInfo() throws PropertyReadException;

    void savePgFacadeLoadBalancerInfo(ExternalLoadBalancerRaftInfo info) throws PropertyModificationException;
}
