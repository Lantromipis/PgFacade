package com.lantromipis.orchestration.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lantromipis.configuration.event.SwitchoverCompletedEvent;
import com.lantromipis.configuration.event.SwitchoverStartedEvent;
import com.lantromipis.configuration.exception.PropertyReadException;
import com.lantromipis.orchestration.exception.RaftException;
import com.lantromipis.orchestration.model.raft.ExternalLoadBalancerRaftInfo;
import com.lantromipis.orchestration.model.raft.PostgresPersistedArchiverInfo;
import com.lantromipis.orchestration.model.raft.PostgresPersistedInstanceInfo;
import com.lantromipis.orchestration.service.api.PgFacadeRaftService;
import com.lantromipis.orchestration.service.api.raft.RaftStorage;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.UUID;

import static com.lantromipis.orchestration.constant.RaftConstants.*;

@Slf4j
@ApplicationScoped
public class RaftFunctionalityCombinator {
    @Inject
    PgFacadeRaftService raftService;

    @Inject
    RaftStorage raftStorage;

    @Inject
    ObjectMapper objectMapper;


    private static final long TIMEOUT = 2000;
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    public boolean testIfAbleToCommitToRaftNoException() {
        try {
            testIfAbleToCommitToRaft();
            return true;
        } catch (Exception e) {
            log.error("Not able to commit to Raft! Is this node a leader?");
            return false;
        }
    }

    public void testIfAbleToCommitToRaft() throws RaftException {
        raftService.appendToLogAndAwaitCommit(
                DUMMY_COMMIT_TEST_COMMAND,
                EMPTY_BYTE_ARRAY,
                TIMEOUT
        );
    }

    public ExternalLoadBalancerRaftInfo getPgFacadeLoadBalancerInfo() throws PropertyReadException {
        return raftStorage.getPgFacadeLoadBalancerInfo();
    }

    public void savePgFacadeLoadBalancerInfo(ExternalLoadBalancerRaftInfo info) throws RaftException {
        raftService.appendToLogAndAwaitCommit(
                SAVE_PGFACADE_LOAD_BALANCER_INFO,
                writeAsBytesSafe(info),
                TIMEOUT
        );
    }

    public void saveArchiverInfoInRaft(PostgresPersistedArchiverInfo archiveInfo) throws RaftException {
        raftService.appendToLogAndAwaitCommit(
                SAVE_POSTGRES_ARCHIVE_INFO,
                writeAsBytesSafe(archiveInfo),
                TIMEOUT
        );
    }

    public PostgresPersistedArchiverInfo getArchiveInfo() throws PropertyReadException {
        return raftStorage.getArchiveInfo();
    }

    public void notifyAllClusterAboutSwitchoverStarted(SwitchoverStartedEvent switchoverStartedEvent) throws RaftException {
        raftService.appendToLogAndAwaitCommit(
                NOTIFY_ALL_CLUSTER_ABOUT_SWITCHOVER_STARTED,
                writeAsBytesSafe(switchoverStartedEvent),
                TIMEOUT
        );
    }

    public void notifyAllClusterAboutSwitchoverCompleted(SwitchoverCompletedEvent switchoverCompletedEvent) throws RaftException {
        raftService.appendToLogAndAwaitCommit(
                NOTIFY_ALL_CLUSTER_ABOUT_SWITCHOVER_COMPLETED,
                writeAsBytesSafe(switchoverCompletedEvent),
                TIMEOUT
        );
    }

    public List<PostgresPersistedInstanceInfo> getPostgresNodeInfos() throws PropertyReadException {
        return raftStorage.getPostgresNodeInfos();
    }

    public PostgresPersistedInstanceInfo getPostgresNodeInfo(UUID instanceId) throws PropertyReadException {
        return raftStorage.getPostgresNodeInfo(instanceId);
    }

    public void savePostgresNodeInfoInRaft(PostgresPersistedInstanceInfo postgresPersistedInstanceInfo) throws RaftException {
        raftService.appendToLogAndAwaitCommit(
                SAVE_POSTGRES_NODE_INFO,
                writeAsBytesSafe(postgresPersistedInstanceInfo),
                TIMEOUT
        );
    }

    public void deletePostgresNodeInfoInRaft(UUID instanceId) throws RaftException {
        raftService.appendToLogAndAwaitCommit(
                DELETE_POSTGRES_NODE_INFO,
                instanceId.toString().getBytes(),
                TIMEOUT
        );
    }

    public void updatePostgresNodeInfoInRaft(PostgresPersistedInstanceInfo updatedPostgresPersistedInstanceInfo) throws RaftException {
        raftService.appendToLogAndAwaitCommit(
                UPDATE_POSTGRES_NODE_INFO,
                writeAsBytesSafe(updatedPostgresPersistedInstanceInfo),
                TIMEOUT
        );
    }

    public void clearPostgresNodesInfosInRaft() throws RaftException {
        raftService.appendToLogAndAwaitCommit(
                CLEAR_POSTGRES_NODES_INFOS,
                EMPTY_BYTE_ARRAY,
                TIMEOUT
        );
    }

    public void notifyClusterAboutSettingsChange() throws RaftException {
        raftService.appendToLogAndAwaitCommit(
                NOTIFY_ABOUT_POSTGRES_SETTINGS_CHANGE,
                EMPTY_BYTE_ARRAY,
                TIMEOUT
        );
    }

    private byte[] writeAsBytesSafe(Object o) throws RaftException {
        try {
            return objectMapper.writeValueAsBytes(o);
        } catch (JsonProcessingException e) {
            throw new RaftException("Error serializing JSON data for raft log", e);
        }
    }
}
