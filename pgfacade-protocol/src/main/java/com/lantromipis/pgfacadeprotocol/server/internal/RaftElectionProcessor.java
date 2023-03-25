package com.lantromipis.pgfacadeprotocol.server.internal;

import com.lantromipis.pgfacadeprotocol.message.VoteRequest;
import com.lantromipis.pgfacadeprotocol.message.VoteResponse;
import com.lantromipis.pgfacadeprotocol.model.api.RaftRole;
import com.lantromipis.pgfacadeprotocol.model.api.RaftServerProperties;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftNodeCallbackInfo;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftPeerWrapper;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftServerContext;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftServerOperationsLog;
import com.lantromipis.pgfacadeprotocol.utils.RaftUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class RaftElectionProcessor {

    private RaftServerContext context;
    private Consumer<RaftNodeCallbackInfo> responseCallback;
    private RaftServerProperties raftServerProperties;

    public RaftElectionProcessor(RaftServerContext context, RaftServerProperties raftServerProperties, Consumer<RaftNodeCallbackInfo> responseCallback) {
        this.context = context;
        this.raftServerProperties = raftServerProperties;
        this.responseCallback = responseCallback;
    }

    public synchronized VoteResponse vote(VoteRequest voteRequest) {
        // Reply false if term < currentTerm (§5.1)
        if (voteRequest.getTerm() < context.getCurrentTerm().get()) {
            return VoteResponse
                    .builder()
                    .groupId(context.getRaftGroupId())
                    .nodeId(context.getSelfNodeId())
                    .term(context.getCurrentTerm().get())
                    .agreed(false)
                    .round(voteRequest.getRound())
                    .build();
        }

        boolean termAgreed;

        if (voteRequest.getTerm() == context.getCurrentTerm().get()) {
            // If not voted or candidateId, and candidate’s operations is at least as up-to-date as receiver’s operations, grant vote (§5.2, §5.4)
            termAgreed = context.getVotedForNodeId() == null || context.getVotedForNodeId().equals(voteRequest.getNodeId());
        } else {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            termAgreed = true;
            processTermGreaterThanCurrent(voteRequest.getTerm());
        }

        RaftServerOperationsLog operationsLog = context.getRaftServerOperationsLog();

        boolean logAgreed = !(
                (
                        operationsLog.getLastTerm().get() > voteRequest.getLastLogTerm()
                )
                        || (
                        operationsLog.getLastTerm().get() == voteRequest.getLastLogTerm()
                                && operationsLog.getLastIndex().get() > voteRequest.getLastLogIndex()
                )
        );

        boolean voteGranted = termAgreed && logAgreed;

        if (voteGranted) {
            context.setVotedForNodeId(voteRequest.getNodeId());
        }

        return VoteResponse
                .builder()
                .groupId(context.getRaftGroupId())
                .nodeId(context.getSelfNodeId())
                .term(context.getCurrentTerm().get())
                .agreed(voteGranted)
                .round(voteRequest.getRound())
                .build();
    }

    public synchronized void processElection() {
        // already leader
        if (context.getSelfRole().equals(RaftRole.LEADER)) {
            return;
        }

        log.info("Starting Raft election!");

        context.setSelfRole(RaftRole.CANDIDATE);
        long startTerm = context.getCurrentTerm().incrementAndGet();
        context.setVotedForNodeId(context.getSelfNodeId());

        // because voted for self
        int grantedVotes = 1;
        int revokeVotes = 0;

        context.getVoteResponses().clear();

        int round = ThreadLocalRandom.current().nextInt();

        Map<String, RaftPeerWrapper> nodesIdsToSend = context.getRaftPeers()
                .values()
                .stream()
                .filter(wrapper -> !wrapper.getRaftNode().getId().equals(context.getSelfNodeId()))
                .collect(
                        Collectors.toMap(
                                wrapper -> wrapper.getRaftNode().getId(),
                                Function.identity()
                        )
                );

        // not count previous leader
        int expectedResponses = nodesIdsToSend.size() - 1;

        while (isBeingElected(startTerm, context)) {
            VoteRequest voteRequest = VoteRequest.builder()
                    .groupId(context.getRaftGroupId())
                    .nodeId(context.getSelfNodeId())
                    .term(context.getCurrentTerm().get())
                    .lastLogTerm(context.getRaftServerOperationsLog().getLastTerm().get())
                    .lastLogIndex(context.getRaftServerOperationsLog().getLastIndex().get())
                    .round(round)
                    .build();

            nodesIdsToSend.values().forEach(wrapper ->
                    RaftUtils.tryToSendMessageToPeer(
                            wrapper,
                            voteRequest,
                            context,
                            raftServerProperties,
                            responseCallback
                    )
            );

            long startTime = System.currentTimeMillis();

            while (System.currentTimeMillis() - startTime < raftServerProperties.getVoteTimeout() || context.getVoteResponses().size() < expectedResponses) {
                // spin lock
            }

            for (var voteResponse : context.getVoteResponses()) {
                if (voteResponse.getRound() != round) {
                    // some old async response or hack
                    context.getVoteResponses().remove(voteResponse);
                }

                if (voteResponse.getTerm() > context.getCurrentTerm().get()) {
                    // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
                    processTermGreaterThanCurrent(voteResponse.getTerm());
                    return;
                }

                if (voteResponse.isAgreed()) {
                    log.debug("Node with id {} GRANTED vote!", voteResponse.getNodeId());
                    grantedVotes++;
                } else {
                    log.debug("Node with id {} NOT GRANTED vote!", voteResponse.getNodeId());
                    revokeVotes++;
                }
                nodesIdsToSend.remove(voteResponse.getNodeId());
            }

            int quorum = RaftUtils.calculateQuorum(context);

            if (isBeingElected(startTerm, context)) {
                if (grantedVotes >= quorum) {
                    log.debug("Election WON!");
                    context.setSelfRole(RaftRole.LEADER);
                    context.getRaftPeers().values()
                            .forEach(wrapper -> wrapper.getNextIndex().set(
                                            context.getRaftServerOperationsLog().getLastIndex().get() + 1
                                    )
                                    //TODO
                            );
                    return;
                } else if (revokeVotes >= quorum) {
                    log.debug("Election LOST");
                    context.setSelfRole(RaftRole.FOLLOWER);
                    return;
                }
                log.debug("Quorum NOT reached! Restarting election...");
            }
        }
    }

    public void processTermGreaterThanCurrent(long term) {
        context.setSelfRole(RaftRole.FOLLOWER);
        context.getCurrentTerm().set(term);
        context.getVoteResponses().clear();
        context.setVotedForNodeId(null);
    }

    private boolean isBeingElected(long term, RaftServerContext context) {
        return term == context.getCurrentTerm().get() && context.getSelfRole().equals(RaftRole.CANDIDATE);
    }
}
