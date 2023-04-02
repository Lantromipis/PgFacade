package com.lantromipis.pgfacadeprotocol.server.internal;

import com.lantromipis.pgfacadeprotocol.message.VoteRequest;
import com.lantromipis.pgfacadeprotocol.message.VoteResponse;
import com.lantromipis.pgfacadeprotocol.model.api.RaftRole;
import com.lantromipis.pgfacadeprotocol.model.api.RaftServerProperties;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftNodeCallbackInfo;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftServerContext;
import com.lantromipis.pgfacadeprotocol.server.impl.RaftTimer;
import com.lantromipis.pgfacadeprotocol.utils.RaftUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

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
            log.debug("Replying FALSE to VOTE REQUEST from node {} because term in request is less than current.", voteRequest.getNodeId());
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

        RaftServerOperationsLog operationsLog = context.getOperationLog();

        boolean logAgreed = !(
                (
                        operationsLog.getLastTerm() > voteRequest.getLastLogTerm()
                )
                        || (
                        operationsLog.getLastTerm() == voteRequest.getLastLogTerm()
                                && operationsLog.getEffectiveLastIndex().get() > voteRequest.getLastLogIndex()
                )
        );

        boolean voteGranted = termAgreed && logAgreed;

        if (voteGranted) {
            log.debug("Granted vote for node {}", voteRequest.getNodeId());
            context.setVotedForNodeId(voteRequest.getNodeId());
        } else {
            log.debug("Not granting vote for node {}. Term agreed: {}, Log agreed: {}", voteRequest.getNodeId(), termAgreed, logAgreed);
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

    public synchronized void processElection(Runnable hearthBeatRunnable) {
        // already leader
        if (context.getSelfRole().equals(RaftRole.LEADER)) {
            return;
        }

        log.info("Starting Raft election!");

        RaftTimer voteTimer = context.getVoteTimer();

        context.setSelfRole(RaftRole.CANDIDATE);
        long startTerm = context.getCurrentTerm().incrementAndGet();
        context.setVotedForNodeId(context.getSelfNodeId());

        voteTimer.pause();

        // because voted for self
        int grantedVotes = 1;
        int revokeVotes = 0;

        context.getVoteResponses().clear();

        long round = System.currentTimeMillis();

        // not count previous leader
        int expectedResponsesCount = context.getRaftPeers().size() - 1;

        VoteRequest voteRequest = VoteRequest.builder()
                .groupId(context.getRaftGroupId())
                .nodeId(context.getSelfNodeId())
                .term(context.getCurrentTerm().get())
                .lastLogTerm(context.getOperationLog().getLastTerm())
                .lastLogIndex(context.getOperationLog().getEffectiveLastIndex().get())
                .round(round)
                .build();

        context.getRaftPeers()
                .values()
                .stream()
                .filter(wrapper -> !wrapper.getRaftNode().getId().equals(context.getSelfNodeId()))
                .forEach(wrapper ->
                        RaftUtils.tryToSendMessageToPeer(
                                wrapper,
                                voteRequest,
                                context,
                                raftServerProperties,
                                responseCallback
                        )
                );

        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < raftServerProperties.getVoteTimeout() && context.getVoteResponses().size() < expectedResponsesCount) {
            // spin lock
        }

        for (var voteResponse : context.getVoteResponses().values()) {
            if (voteResponse.getRound() != round) {
                continue;
            }

            if (voteResponse.getTerm() > context.getCurrentTerm().get()) {
                // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
                processTermGreaterThanCurrent(voteResponse.getTerm());
                voteTimer.resume();
                return;
            }

            if (voteResponse.isAgreed()) {
                log.debug("Node with id {} GRANTED vote!", voteResponse.getNodeId());
                grantedVotes++;
            } else {
                log.debug("Node with id {} NOT GRANTED vote!", voteResponse.getNodeId());
                revokeVotes++;
            }
        }

        int quorum = RaftUtils.calculateQuorum(context);

        if (isBeingElected(startTerm, context)) {
            if (grantedVotes >= quorum) {
                log.debug("Election WON!");
                context.getRaftPeers().values()
                        .forEach(wrapper -> {
                                    RaftUtils.updateIncrementalAtomicLong(wrapper.getNextIndex(), context.getOperationLog().getEffectiveLastIndex().get() + 1);
                                    wrapper.setLastTimeActive(System.currentTimeMillis());
                                }
                        );
                context.setSelfRole(RaftRole.LEADER);
                voteTimer.resume();
                hearthBeatRunnable.run();
                return;
            } else if (revokeVotes >= quorum) {
                log.debug("Election LOST");
                context.setSelfRole(RaftRole.FOLLOWER);
                voteTimer.resume();
                return;
            }
            log.warn("Raft quorum NOT reached! Quorum = {}, granted votes = {}, revoked votes = {}. Are there enough nodes in cluster for quorum? Quorum = N/2 + 1, where N is number of nodes initially. Restarting election...",
                    quorum,
                    grantedVotes,
                    revokeVotes
            );
        } else {
            log.debug("Election was stopped. Most likely other node became leader.");
            voteTimer.resume();
            return;
        }

        long timeToDelay = ThreadLocalRandom.current()
                .nextInt(
                        raftServerProperties.getElectionRestartTimeoutMax() - raftServerProperties.getElectionRestartTimeoutMin()
                ) + raftServerProperties.getElectionRestartTimeoutMin();

        try {
            Thread.sleep(timeToDelay);
        } catch (InterruptedException ignored) {
        }

        voteTimer.resume();
    }

    public void processTermGreaterThanCurrent(long term) {
        context.setSelfRole(RaftRole.FOLLOWER);
        context.getCurrentTerm().getAndSet(term);
        context.getVoteResponses().clear();
        context.setVotedForNodeId(null);
    }

    private boolean isBeingElected(long term, RaftServerContext context) {
        return term == context.getCurrentTerm().get() && context.getSelfRole().equals(RaftRole.CANDIDATE);
    }
}
