package com.lantromipis.pgfacadeprotocol.server.internal;

import com.lantromipis.pgfacadeprotocol.constant.InternalRaftCommandsConstants;
import com.lantromipis.pgfacadeprotocol.model.api.RaftNode;
import com.lantromipis.pgfacadeprotocol.model.api.RaftServerProperties;
import com.lantromipis.pgfacadeprotocol.model.internal.LogEntry;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftPeerWrapper;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftServerContext;
import com.lantromipis.pgfacadeprotocol.utils.InternalCommandsEncoderDecoder;
import com.lantromipis.pgfacadeprotocol.utils.RaftUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Objects;

@Slf4j
public class RaftCommitProcessor {
    private RaftServerContext context;
    private RaftServerProperties properties;

    public RaftCommitProcessor(RaftServerContext context, RaftServerProperties properties) {
        this.context = context;
        this.properties = properties;
    }

    public boolean leaderCommit() {
        if (!context.getCommitInProgress().compareAndSet(false, true)) {
            // some thread already committing
            return false;
        }

        try {

            // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
            // and operations[N].term == currentTerm:
            // set commitIndex = N (§5.3, §5.4).

            var operationLog = context.getOperationLog();

            long n = context.getCommitIndex().get() + 1;
            int committedCount = 0;

            while (true) {
                long finalN = n;
                long peersWithSuchN = context.getRaftPeers().values()
                        .stream()
                        .map(RaftPeerWrapper::getMatchIndex)
                        .filter(matchIndex -> matchIndex.get() >= finalN)
                        .count();
                // count self
                peersWithSuchN++;

                if (operationLog.getLastIndex().get() >= n && peersWithSuchN >= RaftUtils.calculateQuorum(context)) {
                    // do not commit previous leader
                    if (!Objects.equals(operationLog.getTerm(n), context.getCurrentTerm().get())) {
                        n++;
                        continue;
                    }
                    context.getCommitIndex().getAndSet(n);
                    committedCount++;
                    operationLog.getCommitsFromLastShrink().incrementAndGet();

                    callStateMachine(n);

                    n++;
                } else {
                    long commitsFromLastShrink = operationLog.getCommitsFromLastShrink().get();

                    if (commitsFromLastShrink > properties.getShrinkLogEveryNumOfCommits()) {
                        long lastIndex = context.getCommitIndex().get() - 1;

                        operationLog.shrinkLog(lastIndex);
                        operationLog.getCommitsFromLastShrink().getAndSet(0);

                        // TODO remove
                        log.info("LEADER COMMIT SHRINKS. NEW SIZE {} FIRST INDEX {}", operationLog.getOperationsLog().size(), operationLog.getFirstIndexInLog().get());
                    }
                    return committedCount > 0;
                }
            }
        } finally {
            context.getCommitInProgress().set(false);
        }
    }

    public void followerCommit(long leaderCommit) {
        var operationLog = context.getOperationLog();

        long lastIndex = operationLog.getLastIndex().get();
        long commitIndex = Math.min(leaderCommit, lastIndex);
        context.getCommitIndex().getAndSet(commitIndex);

        callStateMachine(commitIndex);

        if (!context.isNotifiedStartupSync() && commitIndex == leaderCommit) {
            log.info("Log synced with leader! Raft server is fully ready.");
            context.getEventListener().syncedWithLeaderOrSelfIsLeaderOnStartup();
            context.setNotifiedStartupSync(true);
        }
    }

    private void callStateMachine(long commitIndex) {
        if (context.getRaftStateMachine() != null) {
            LogEntry logEntry = context.getOperationLog().getLogEntry(commitIndex);

            if (logEntry == null) {
                log.error("Missing committed command at index {}!", commitIndex);
                return;
            }

            String command = logEntry.getCommand();

            if (InternalRaftCommandsConstants.ADD_NEW_RAFT_NODE_COMMAND.equals(command)) {
                RaftNode raftNode = InternalCommandsEncoderDecoder.decodeAddRaftNodeCommandData(logEntry.getData());

                if (!raftNode.getId().equals(context.getSelfNodeId())) {
                    context.getRaftPeers()
                            .put(
                                    raftNode.getId(),
                                    new RaftPeerWrapper(
                                            RaftNode.builder()
                                                    .id(raftNode.getId())
                                                    .groupId(context.getRaftGroupId())
                                                    .ipAddress(raftNode.getIpAddress())
                                                    .port(raftNode.getPort())
                                                    .build()
                                    )
                            );
                }
            } else {
                context.getRaftStateMachine().operationCommitted(
                        commitIndex,
                        logEntry.getCommand(),
                        Arrays.copyOf(logEntry.getData(), logEntry.getData().length)
                );
            }
        }
        context.getStateMachineApplyIndex().getAndSet(commitIndex);
    }
}
