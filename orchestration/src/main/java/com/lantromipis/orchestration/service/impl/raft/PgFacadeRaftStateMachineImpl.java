package com.lantromipis.orchestration.service.impl.raft;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lantromipis.configuration.event.SwitchoverCompletedEvent;
import com.lantromipis.configuration.event.SwitchoverStartedEvent;
import com.lantromipis.configuration.model.PostgresPersistedInstanceInfo;
import com.lantromipis.orchestration.exception.RaftException;
import com.lantromipis.orchestration.service.api.raft.PgFacadeRaftStateMachine;
import com.lantromipis.orchestration.service.api.raft.RaftStorage;
import com.lantromipis.orchestration.util.RaftCommitUtils;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.lantromipis.orchestration.constant.RaftCommandConstants.*;

@Slf4j
@ApplicationScoped
public class PgFacadeRaftStateMachineImpl implements PgFacadeRaftStateMachine {

    @Inject
    RaftStorage raftStorage;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    RaftCommitUtils raftCommitUtils;

    private ConcurrentMap<Long, CountDownLatch> commitIndexLatches = new ConcurrentHashMap<>();

    private AtomicLong lastCommitIdx = new AtomicLong(-1);

    @Override
    public void operationCommitted(long commitIndex, String command, byte[] data) {

        try {
            switch (command) {
                case SAVE_POSTGRES_NODE_INFO -> {
                    PostgresPersistedInstanceInfo postgresPersistedInstanceInfo = objectMapper.readValue(new String(data), PostgresPersistedInstanceInfo.class);
                    raftStorage.savePostgresNodeInfo(postgresPersistedInstanceInfo);
                    raftCommitUtils.processCommittedSavePostgresNodeInfoCommand(postgresPersistedInstanceInfo);
                }
                case DELETE_POSTGRES_NODE_INFO -> {
                    UUID instanceId = UUID.fromString(new String(data));
                    raftStorage.deletePostgresNodeInfo(instanceId);
                    raftCommitUtils.processCommittedDeletePostgresNodeInfoCommand(instanceId);
                }
                case CLEAR_POSTGRES_NODES_INFOS -> {
                    raftStorage.clearPostgresNodesInfos();
                    raftCommitUtils.processCommittedClearPostgresNodeInfoCommand();
                }
                case SAVE_POSTGRES_SETTINGS_INFO -> {
                    Map<String, String> persistedSettingsInfos = objectMapper.readValue(
                            new String(data),
                            RaftFileBasedStorage.POSTGRES_SETTING_INFO_TYPE_REF
                    );
                    raftStorage.savePostgresSettingsInfos(persistedSettingsInfos);
                    raftCommitUtils.processCommittedPostgresSettingsInfoCommand(persistedSettingsInfos);
                }
                case NOTIFY_ALL_CLUSTER_ABOUT_SWITCHOVER_STARTED -> {
                    SwitchoverStartedEvent switchoverStartedEvent = objectMapper.readValue(new String(data), SwitchoverStartedEvent.class);
                    raftCommitUtils.processCommittedSwitchoverStartedEvent(switchoverStartedEvent);
                }
                case NOTIFY_ALL_CLUSTER_ABOUT_SWITCHOVER_COMPLETED -> {
                    SwitchoverCompletedEvent switchoverCompletedEvent = objectMapper.readValue(new String(data), SwitchoverCompletedEvent.class);
                    raftCommitUtils.processCommittedSwitchoverCompletedEvent(switchoverCompletedEvent);
                }
                case DUMMY_COMMIT_TEST_COMMAND -> {
                    // do nothing...
                }
                default -> {
                    log.warn("Unknown raft command {}", command);
                }
            }
            log.debug("Committed command {}", command);
        } catch (JsonProcessingException e) {
            // Corner case. Leader must serialize object, before appending it to Raft log.
            log.warn("Failed to save committed operation!", e);
        }

        lastCommitIdx.set(commitIndex);
        Optional.ofNullable(commitIndexLatches.get(commitIndex)).ifPresent(CountDownLatch::countDown);
    }

    @Override
    public void awaitCommit(long operationIndex, long timeoutMs) throws InterruptedException, RaftException {
        try {
            CountDownLatch latch = new CountDownLatch(1);
            commitIndexLatches.put(operationIndex, latch);

            if (lastCommitIdx.get() >= operationIndex) {
                return;
            }

            if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                throw new RaftException("Timeout reached while waiting for operation to commit!");
            }
        } finally {
            commitIndexLatches.remove(operationIndex);
        }
    }
}
