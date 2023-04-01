package com.lantromipis.orchestration.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lantromipis.configuration.event.SwitchoverCompletedEvent;
import com.lantromipis.configuration.event.SwitchoverStartedEvent;
import com.lantromipis.configuration.exception.PropertyReadException;
import com.lantromipis.configuration.model.PostgresPersistedInstanceInfo;
import com.lantromipis.orchestration.exception.RaftException;
import com.lantromipis.orchestration.service.api.PgFacadeRaftService;
import com.lantromipis.orchestration.service.api.raft.RaftStorage;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
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
                new byte[0],
                TIMEOUT
        );
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

    public Map<String, String> getPostgresSettingInfos() throws PropertyReadException {
        return raftStorage.getPostgresSettingInfos();
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

    public void clearPostgresNodesInfosInRaft() throws RaftException {
        raftService.appendToLogAndAwaitCommit(
                CLEAR_POSTGRES_NODES_INFOS,
                new byte[0],
                TIMEOUT
        );
    }

    public void savePostgresSettingsInfosInRaft(Map<String, String> persistedSettingsInfos) throws RaftException {
        raftService.appendToLogAndAwaitCommit(
                SAVE_POSTGRES_SETTINGS_INFO,
                writeAsBytesSafe(persistedSettingsInfos),
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
