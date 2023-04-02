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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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

                if (operationLog.getEffectiveLastIndex().get() >= n && peersWithSuchN >= RaftUtils.calculateQuorum(context)) {
                    // do not commit previous leader
                    if (!Objects.equals(operationLog.getTerm(n), context.getCurrentTerm().get())) {
                        n++;
                        continue;
                    }
                    RaftUtils.updateIncrementalAtomicLong(context.getCommitIndex(), n);
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

        long lastIndex = operationLog.getEffectiveLastIndex().get();
        long commitIndex = Math.min(leaderCommit, lastIndex);
        RaftUtils.updateIncrementalAtomicLong(context.getCommitIndex(), commitIndex);

        callStateMachine(commitIndex);

        if (!context.getNotifiedStartupSync().get() && commitIndex == leaderCommit && context.getNotifiedStartupSync().compareAndSet(false, true)) {
            context.getEventListener().syncedWithLeaderOrSelfIsLeaderOnStartup();
        }
    }

    private void callStateMachine(long commitIndex) {
        if (context.getStateMachineApplyIndex().get() < commitIndex && context.getRaftStateMachine() != null) {
            LogEntry logEntry = context.getOperationLog().getLogEntry(commitIndex);

            String command = logEntry.getCommand();

            if (InternalRaftCommandsConstants.UPDATE_RAFT_MEMBERSHIP_COMMAND.equals(command)) {
                List<RaftNode> newMembership = InternalCommandsEncoderDecoder.decodeUpdateRaftMembershipCommandData(logEntry.getData());

                Map<String, RaftPeerWrapper> newMembershipWithoutSelf = newMembership
                        .stream()
                        .filter(node -> !node.getId().equals(context.getSelfNodeId()))
                        .collect(
                                Collectors.toMap(
                                        RaftNode::getId,
                                        node -> new RaftPeerWrapper(
                                                RaftNode.builder()
                                                        .id(node.getId())
                                                        .groupId(node.getGroupId())
                                                        .ipAddress(node.getIpAddress())
                                                        .port(node.getPort())
                                                        .build()
                                        )
                                )
                        );

                context.getRaftPeers().clear();
                context.getRaftPeers().putAll(newMembershipWithoutSelf);

                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("Updated Raft membership. New membership with peers: ");

                for (var peer : context.getRaftPeers().values()) {
                    stringBuilder.append("{").append(peer.getRaftNode().getId()).append("} ");
                }

                log.debug(stringBuilder.toString());

            } else {
                context.getRaftStateMachine().operationCommitted(
                        commitIndex,
                        logEntry.getCommand(),
                        Arrays.copyOf(logEntry.getData(), logEntry.getData().length)
                );
            }
        }
        RaftUtils.updateIncrementalAtomicLong(context.getStateMachineApplyIndex(), commitIndex);
    }
}
