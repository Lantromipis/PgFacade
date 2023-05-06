package com.lantromipis.orchestration.service.impl.raft;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lantromipis.configuration.event.SwitchoverCompletedEvent;
import com.lantromipis.configuration.event.SwitchoverStartedEvent;
import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.orchestration.exception.RaftException;
import com.lantromipis.orchestration.model.raft.ExternalLoadBalancerRaftInfo;
import com.lantromipis.orchestration.model.raft.PostgresPersistedArchiveInfo;
import com.lantromipis.orchestration.model.raft.PostgresPersistedInstanceInfo;
import com.lantromipis.orchestration.service.api.raft.PgFacadeRaftStateMachine;
import com.lantromipis.orchestration.service.api.raft.RaftStorage;
import com.lantromipis.orchestration.util.RaftCommitUtils;
import com.lantromipis.pgfacadeprotocol.model.api.SnapshotChunk;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.lantromipis.orchestration.constant.RaftConstants.*;

@Slf4j
@ApplicationScoped
public class PgFacadeRaftStateMachineImpl implements PgFacadeRaftStateMachine {

    @Inject
    RaftStorage raftStorage;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    RaftCommitUtils raftCommitUtils;

    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

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
                case SAVE_POSTGRES_ARCHIVE_INFO -> {
                    PostgresPersistedArchiveInfo archiveInfo = objectMapper.readValue(new String(data), PostgresPersistedArchiveInfo.class);
                    raftCommitUtils.processArchiveInfoSave(archiveInfo);
                }
                case SAVE_PGFACADE_LOAD_BALANCER_INFO -> {
                    ExternalLoadBalancerRaftInfo loadBalancerInfo = objectMapper.readValue(new String(data), ExternalLoadBalancerRaftInfo.class);
                    raftCommitUtils.processPgFacadeLoadBalancerInfoSave(loadBalancerInfo);
                }
                default -> {
                    log.warn("Unknown raft command {}", command);
                }
            }
            if (pgFacadeRuntimeProperties.getRaftRole().equals(PgFacadeRaftRole.LEADER)) {
                log.debug("Committed command {} index {}", command, commitIndex);
            }
        } catch (JsonProcessingException e) {
            // Corner case. Leader must serialize object, before appending it to Raft log.
            log.warn("Failed to save committed operation!", e);
        }

        lastCommitIdx.getAndSet(commitIndex);
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

    @Override
    public void takeSnapshot(long commitIndex, Consumer<SnapshotChunk> snapshotChunkConsumer) {
        log.debug("Creating snapshot at index {}", commitIndex);
        raftStorage.getChunks().forEach(snapshotChunkConsumer);
    }

    @Override
    public void installSnapshot(long commitIndex, List<SnapshotChunk> snapshotChunks) {
        log.debug("Installing snapshot with last index {}", commitIndex);
        raftStorage.loadChunks(snapshotChunks);
        raftCommitUtils.processInstallSnapshot();
    }
}
