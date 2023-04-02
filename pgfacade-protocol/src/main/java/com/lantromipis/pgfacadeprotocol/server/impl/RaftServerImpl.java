package com.lantromipis.pgfacadeprotocol.server.impl;

import com.lantromipis.pgfacadeprotocol.constant.InternalRaftCommandsConstants;
import com.lantromipis.pgfacadeprotocol.exception.ConfigurationException;
import com.lantromipis.pgfacadeprotocol.exception.NotActiveException;
import com.lantromipis.pgfacadeprotocol.exception.NotLeaderException;
import com.lantromipis.pgfacadeprotocol.message.*;
import com.lantromipis.pgfacadeprotocol.model.api.*;
import com.lantromipis.pgfacadeprotocol.model.internal.LogEntry;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftNodeCallbackInfo;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftPeerWrapper;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftServerContext;
import com.lantromipis.pgfacadeprotocol.netty.RaftServerChannelInitializer;
import com.lantromipis.pgfacadeprotocol.server.api.RaftEventListener;
import com.lantromipis.pgfacadeprotocol.server.api.RaftServer;
import com.lantromipis.pgfacadeprotocol.server.api.RaftStateMachine;
import com.lantromipis.pgfacadeprotocol.server.internal.RaftCommitProcessor;
import com.lantromipis.pgfacadeprotocol.server.internal.RaftElectionProcessor;
import com.lantromipis.pgfacadeprotocol.server.internal.RaftServerOperationsLog;
import com.lantromipis.pgfacadeprotocol.utils.InternalCommandsEncoderDecoder;
import com.lantromipis.pgfacadeprotocol.utils.NettyUtils;
import com.lantromipis.pgfacadeprotocol.utils.RaftUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.lantromipis.pgfacadeprotocol.constant.MessageMarkerConstants.*;

@Slf4j
public class RaftServerImpl implements RaftServer {

    private RaftServerProperties properties;
    private RaftServerContext context;

    private ChannelFuture serverChannelFuture;
    private RaftElectionProcessor electionProcessor;
    private RaftCommitProcessor commitProcessor;

    public RaftServerImpl(EventLoopGroup bossGroup, EventLoopGroup workerGroup, RaftGroup raftGroup, String selfRaftNodeId, RaftServerProperties raftServerProperties, RaftEventListener raftEventListener, RaftStateMachine raftStateMachine) throws ConfigurationException {
        properties = raftServerProperties;

        context = new RaftServerContext();
        context.setBossGroup(bossGroup);
        context.setWorkerGroup(workerGroup);
        context.setRaftGroupId(raftGroup.getGroupId());

        if (CollectionUtils.isNotEmpty(raftGroup.getRaftNodes())) {
            raftGroup.getRaftNodes()
                    .stream()
                    .filter(raftNode -> !raftNode.getId().equals(selfRaftNodeId))
                    .forEach(
                            item -> {
                                RaftPeerWrapper wrapper = new RaftPeerWrapper(
                                        RaftNode.builder()
                                                .id(item.getId())
                                                .groupId(raftGroup.getGroupId())
                                                .ipAddress(item.getIpAddress())
                                                .port(item.getPort())
                                                .build()
                                );
                                context.getRaftPeers().put(item.getId(), wrapper);
                            }
                    );
        }

        RaftNode selfNode = raftGroup.getRaftNodes()
                .stream()
                .filter(node -> node.getId().equals(selfRaftNodeId))
                .findFirst()
                .orElseThrow(() -> new ConfigurationException("RaftGroup does not contain node with provided selfId!"));

        context.setSelfNodeId(selfRaftNodeId);
        context.setSelfNode(selfNode);
        context.setSelfRole(RaftRole.FOLLOWER);
        context.setEventListener(raftEventListener);
        context.setRaftStateMachine(raftStateMachine);
        context.setOperationLog(new RaftServerOperationsLog());

        electionProcessor = new RaftElectionProcessor(
                context,
                properties,
                this::processRaftNodeResponseCallback
        );
        commitProcessor = new RaftCommitProcessor(
                context,
                properties
        );
    }

    @Override
    public void start() throws InterruptedException {
        if (context.isActive()) {
            return;
        }

        ServerBootstrap serverBootstrap = new ServerBootstrap();

        serverChannelFuture = serverBootstrap
                .group(context.getBossGroup(), context.getWorkerGroup())
                .channel(Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .childHandler(new RaftServerChannelInitializer(this::processRaftNodeRequestCallback))
                .childOption(ChannelOption.AUTO_READ, false)
                .bind(properties.getPort())
                .sync();

        context.setHeartbeatTimer(
                new RaftTimer(
                        0,
                        properties.getHeartbeatTimeout(),
                        this::heartBeat,
                        () -> context.getSelfRole().equals(RaftRole.LEADER)
                )
        );

        context.setVoteTimer(new RaftTimer(
                        properties.getStartupLeaderHeartbeatAwait(),
                        properties.getVoteTimeout(),
                        () -> electionProcessor.processElection(this::heartBeat),
                        () -> context.getSelfRole().equals(RaftRole.FOLLOWER)
                )
        );

        context.getVoteTimer().start();
        context.getHeartbeatTimer().start();
        context.setActive(true);
    }

    @Override
    public void addNewNode(RaftNode raftNode) throws NotLeaderException, NotActiveException {
        if (raftNode.getId().equals(context.getSelfNodeId())) {
            return;
        }

        RaftNode newRaftNode = RaftNode.builder()
                .id(raftNode.getId())
                .groupId(context.getRaftGroupId())
                .ipAddress(raftNode.getIpAddress())
                .port(raftNode.getPort())
                .build();

        Map<String, RaftNode> newMembership = context.getRaftPeers().values()
                .stream()
                .map(RaftPeerWrapper::getRaftNode)
                .collect(
                        Collectors.toMap(
                                RaftNode::getId,
                                Function.identity()
                        )
                );

        newMembership.put(context.getSelfNodeId(), context.getSelfNode());
        newMembership.put(raftNode.getId(), newRaftNode);

        updateMembership(newMembership);
    }

    @Override
    public void removeNode(String nodeId) throws NotLeaderException, NotActiveException {
        if (nodeId.equals(context.getSelfNodeId())) {
            return;
        }

        Map<String, RaftNode> newMembership = context.getRaftPeers().values()
                .stream()
                .map(RaftPeerWrapper::getRaftNode)
                .collect(
                        Collectors.toMap(
                                RaftNode::getId,
                                Function.identity()
                        )
                );

        newMembership.put(context.getSelfNodeId(), context.getSelfNode());
        newMembership.remove(nodeId);

        updateMembership(newMembership);
    }

    @Override
    public List<RaftPeerInfo> getRaftPeers() {
        return context.getRaftPeers().values()
                .stream()
                .map(wrapper -> RaftPeerInfo
                        .builder()
                        .id(wrapper.getRaftNode().getId())
                        .groupId(wrapper.getRaftNode().getGroupId())
                        .ipAddress(wrapper.getRaftNode().getIpAddress())
                        .port(wrapper.getRaftNode().getPort())
                        .lastTimeActive(wrapper.getLastTimeActive())
                        .build()
                )
                .collect(Collectors.toList());
    }

    @Override
    public void shutdown() {
        context.setActive(false);
        context.getVoteTimer().stop();
        context.getHeartbeatTimer().stop();
        serverChannelFuture.channel().close().syncUninterruptibly();
    }

    @Override
    public long appendToLog(String command, byte[] data) throws NotLeaderException, NotActiveException {
        if (!context.isActive()) {
            throw new NotActiveException();
        }

        if (!context.getSelfRole().equals(RaftRole.LEADER)) {
            throw new NotLeaderException();
        }

        long index = context.getOperationLog().append(
                context.getCurrentTerm().get(),
                command,
                data
        );

        // for cases when this raft node is alone
        if (context.getRaftPeers().isEmpty()) {
            commitProcessor.leaderCommit();
        }

        heartBeat();

        return index;
    }

    private void updateMembership(Map<String, RaftNode> newMembership) throws NotLeaderException, NotActiveException {
        context.getOperationLog().getCommitsFromLastShrink().set(0);

        appendToLog(
                InternalRaftCommandsConstants.UPDATE_RAFT_MEMBERSHIP_COMMAND,
                InternalCommandsEncoderDecoder.encodeUpdateRaftMembershipCommandData(newMembership.values().stream().toList())
        );

        commitProcessor.leaderCommit();
    }

    private void heartBeat() {
        for (var wrapper : context.getRaftPeers().values()) {
            long nextPeerIndex = wrapper.getNextIndex().get();
            long prevPeerIndex = nextPeerIndex - 1;
            long lastIndex = context.getOperationLog().getEffectiveLastIndex().get();
            long firstIndex = context.getOperationLog().getFirstIndexInLog().get();
            long commitIndex = context.getCommitIndex().get();

            AbstractMessage message;

            if (prevPeerIndex < firstIndex) {
                List<SnapshotChunk> chunks = new ArrayList<>();
                context.getRaftStateMachine().takeSnapshot(
                        lastIndex,
                        chunks::add
                );

                message = InstallSnapshotRequest
                        .builder()
                        .groupId(context.getRaftGroupId())
                        .nodeId(context.getSelfNodeId())
                        .term(context.getCurrentTerm().get())
                        .leaderId(context.getSelfNodeId())
                        .leaderCommit(commitIndex)
                        .lastIncludedIndex(commitIndex)
                        .chunks(chunks.
                                stream()
                                .map(chunk -> InstallSnapshotRequest.ChunkData
                                        .builder()
                                        .chunkName(chunk.getName())
                                        .data(chunk.getData())
                                        .build()
                                )
                                .collect(Collectors.toList()))
                        .build();
            } else {
                List<AppendRequest.Operation> operations = new ArrayList<>();

                if (nextPeerIndex <= lastIndex) {
                    LogEntry logEntry = context.getOperationLog().getLogEntry(nextPeerIndex);
                    // only single operation for now for stability
                    operations.add(
                            AppendRequest.Operation
                                    .builder()
                                    .term(logEntry.getTerm())
                                    .index(logEntry.getIndex())
                                    .command(logEntry.getCommand())
                                    .data(Arrays.copyOf(logEntry.getData(), logEntry.getData().length))
                                    .build()
                    );
                }

                message = AppendRequest
                        .builder()
                        .groupId(context.getRaftGroupId())
                        .nodeId(context.getSelfNodeId())
                        .previousLogIndex(prevPeerIndex)
                        .previousTerm(context.getOperationLog().getTerm(prevPeerIndex))
                        .currentTerm(context.getCurrentTerm().get())
                        .leaderCommit(commitIndex)
                        .operations(operations)
                        .shrinkIndex(firstIndex)
                        .build();
            }

            RaftUtils.tryToSendMessageToPeer(
                    wrapper,
                    message,
                    context,
                    properties,
                    this::processRaftNodeResponseCallback
            );
        }
    }

    private void processRaftNodeResponseCallback(RaftNodeCallbackInfo callbackInfo) {
        AbstractMessage message = callbackInfo.getMessage();

        if (message instanceof UnknownMessage) {
            NettyUtils.writeAndFlushIfChannelActive(
                    callbackInfo.getChannel(),
                    RejectResponse
                            .builder()
                            .groupId("no-group-id")
                            .nodeId("no-node-id")
                            .build()
            );
            return;
        }

        // security
        if (!context.getRaftGroupId().equals(message.getGroupId()) || !context.getRaftPeers().containsKey(message.getNodeId())) {
            //log.debug("Rejecting request. Node with group id '{}' and node id '{}' is not in current conf.", message.getGroupId(), message.getNodeId());

            NettyUtils.writeAndFlushIfChannelActive(
                    callbackInfo.getChannel(),
                    RejectResponse
                            .builder()
                            .groupId("no-group-id")
                            .nodeId("no-node-id")
                            .build()
            );
            return;
        }

        Optional.ofNullable(context.getRaftPeers().get(message.getNodeId()))
                .ifPresent(peer -> peer.setLastTimeActive(System.currentTimeMillis()));

        switch (message.getMessageMarker()) {
            case VOTE_RESPONSE_MESSAGE_MARKER -> {
                VoteResponse voteResponse = (VoteResponse) callbackInfo.getMessage();
                context.getVoteResponses()
                        .merge(
                                voteResponse.getNodeId(),
                                voteResponse,
                                (oldVal, newVal) -> {
                                    if (oldVal.getRound() <= newVal.getRound()) {
                                        return newVal;
                                    } else {
                                        return oldVal;
                                    }
                                }
                        );
            }
            case APPEND_RESPONSE_MESSAGE_MARKER -> {
                processAppendResponse(callbackInfo);
            }
            case INSTALL_SNAPSHOT_RESPONSE_MESSAGE_MARKER -> {
                processInstallSnapshotResponse(callbackInfo);
            }
        }
    }

    private void processRaftNodeRequestCallback(RaftNodeCallbackInfo callbackInfo) {
        AbstractMessage message = callbackInfo.getMessage();

        if (message instanceof UnknownMessage) {
            //log.debug("Rejecting request with unknown message");

            NettyUtils.writeAndFlushIfChannelActive(
                    callbackInfo.getChannel(),
                    RejectResponse
                            .builder()
                            .groupId("no-group-id")
                            .nodeId("no-node-id")
                            .build()
            );
            return;
        }

        // security
        if (!message.getGroupId().equals(context.getRaftGroupId()) || !context.getRaftPeers().containsKey(message.getNodeId())) {
            //log.debug("Rejecting request. Node with group id '{}' and node id '{}' is not in current conf.", message.getGroupId(), message.getNodeId());

            NettyUtils.writeAndFlushIfChannelActive(
                    callbackInfo.getChannel(),
                    RejectResponse
                            .builder()
                            .groupId("no-group-id")
                            .nodeId("no-node-id")
                            .build()
            );
            return;
        }

        Optional.ofNullable(context.getRaftPeers().get(message.getNodeId()))
                .ifPresent(peer -> peer.setLastTimeActive(System.currentTimeMillis()));

        switch (message.getMessageMarker()) {
            case VOTE_REQUEST_MESSAGE_MARKER -> {
                //log.trace("Received VOTE REQUEST from node {}", message.getNodeId());

                NettyUtils.writeAndFlushIfChannelActive(
                        callbackInfo.getChannel(),
                        electionProcessor.vote((VoteRequest) message)
                );
            }
            case APPEND_REQUEST_MESSAGE_MARKER -> {
                //log.trace("Received APPEND REQUEST from node {}", message.getNodeId());
                processAppendRequest(callbackInfo);
            }
            case INSTALL_SNAPSHOT_REQUEST_MESSAGE_MARKER -> {
                processInstallSnapshotRequest(callbackInfo);
            }
        }
    }

    private void processAppendResponse(RaftNodeCallbackInfo callbackInfo) {
        AppendResponse appendResponse = (AppendResponse) callbackInfo.getMessage();

        if (appendResponse.getTerm() > context.getCurrentTerm().get()) {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            electionProcessor.processTermGreaterThanCurrent(appendResponse.getTerm());
            return;
        }

        RaftPeerWrapper wrapper = context.getRaftPeers().get(appendResponse.getNodeId());
        if (appendResponse.isSuccess()) {
            wrapper.getNextIndex().incrementAndGet();
            RaftUtils.updateIncrementalAtomicLong(wrapper.getMatchIndex(), appendResponse.getMatchIndex());
            RaftUtils.updateIncrementalAtomicLong(wrapper.getCommitIndex(), appendResponse.getCommitIndex());
        } else {
            // failed, decrement and retry
            wrapper.getNextIndex().decrementAndGet();
        }

        if (commitProcessor.leaderCommit()) {
            heartBeat();
        }
    }

    private synchronized void processAppendRequest(RaftNodeCallbackInfo callbackInfo) {
        AppendRequest appendRequest = (AppendRequest) callbackInfo.getMessage();

        // Reply false if term < currentTerm(§ 5.1)
        if (appendRequest.getCurrentTerm() < context.getCurrentTerm().get()) {
            NettyUtils.writeAndFlushIfChannelActive(
                    callbackInfo.getChannel(),
                    AppendResponse
                            .builder()
                            .groupId(context.getRaftGroupId())
                            .nodeId(context.getSelfNodeId())
                            .term(context.getCurrentTerm().get())
                            .success(false)
                            .matchIndex(-1)
                            .commitIndex(-1)
                            .build()
            );
            return;
        }

        context.getVoteTimer().reset();

        if (appendRequest.getCurrentTerm() > context.getCurrentTerm().get()) {
            //If RPC request or response contains term T > currentTerm: set currentTerm = T
            electionProcessor.processTermGreaterThanCurrent(appendRequest.getCurrentTerm());
        }

        if (!context.getSelfRole().equals(RaftRole.FOLLOWER)) {
            context.setSelfRole(RaftRole.FOLLOWER);
        }

        // Reply false if operations does not contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
        if (appendRequest.getPreviousLogIndex() > context.getOperationLog().getEffectiveLastIndex().get()
                || (
                context.getOperationLog().containsIndex(appendRequest.getPreviousLogIndex())
                        && appendRequest.getPreviousTerm() != context.getOperationLog().getTerm(appendRequest.getPreviousLogIndex()))
        ) {
            NettyUtils.writeAndFlushIfChannelActive(
                    callbackInfo.getChannel(),
                    AppendResponse
                            .builder()
                            .groupId(context.getRaftGroupId())
                            .nodeId(context.getSelfNodeId())
                            .term(context.getCurrentTerm().get())
                            .success(false)
                            .matchIndex(-1)
                            .commitIndex(-1)
                            .build()
            );
            return;
        }

        var operationLog = context.getOperationLog();

        if (CollectionUtils.isNotEmpty(appendRequest.getOperations())) {
            // only one operation for now for stability
            AppendRequest.Operation operation = appendRequest.getOperations().get(0);
            long newOperationIndex = appendRequest.getPreviousLogIndex() + 1;

            // If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
            if (newOperationIndex <= operationLog.getEffectiveLastIndex().get()
                    && operation.getTerm() != operationLog.getTerm(newOperationIndex)) {
                operationLog.removeAllStartingFrom(newOperationIndex);
            }

            // Append any new entries not already in the operations
            if (newOperationIndex > operationLog.getEffectiveLastIndex().get()) {
                long index = operationLog.append(
                        operation.getTerm(),
                        operation.getCommand(),
                        operation.getData()
                );
            }
        }

        if (appendRequest.getLeaderCommit() > context.getCommitIndex().get()) {
            commitProcessor.followerCommit(appendRequest.getLeaderCommit());
        }

        if (operationLog.getEffectiveLastIndex().get() > appendRequest.getShrinkIndex()
                && operationLog.getFirstIndexInLog().get() < appendRequest.getShrinkIndex()
                && context.getCommitIndex().get() >= appendRequest.getLeaderCommit()) {
            if (context.getStateMachineApplyIndex().get() >= appendRequest.getShrinkIndex()) {
                operationLog.shrinkLog(appendRequest.getShrinkIndex());
            }
        }

        NettyUtils.writeAndFlushIfChannelActive(
                callbackInfo.getChannel(),
                AppendResponse
                        .builder()
                        .groupId(context.getRaftGroupId())
                        .nodeId(context.getSelfNodeId())
                        .term(context.getCurrentTerm().get())
                        .success(true)
                        .matchIndex(operationLog.getEffectiveLastIndex().get())
                        .commitIndex(context.getCommitIndex().get())
                        .build()
        );
    }

    private void processInstallSnapshotRequest(RaftNodeCallbackInfo callbackInfo) {
        InstallSnapshotRequest request = (InstallSnapshotRequest) callbackInfo.getMessage();

        // Reply false if term < currentTerm(§ 5.1)
        if (request.getTerm() < context.getCurrentTerm().get()) {
            NettyUtils.writeAndFlushIfChannelActive(
                    callbackInfo.getChannel(),
                    InstallSnapshotResponse
                            .builder()
                            .groupId(context.getRaftGroupId())
                            .nodeId(context.getSelfNodeId())
                            .term(context.getCurrentTerm().get())
                            .lastIndex(-1)
                            .success(false)
                            .build()
            );
            return;
        }
        context.getVoteTimer().reset();

        if (request.getLastIncludedIndex() > context.getOperationLog().getEffectiveLastIndex().get()) {
            if (context.getRaftStateMachine() != null) {
                context.getRaftStateMachine().installSnapshot(
                        request.getLastIncludedIndex(),
                        request.getChunks()
                                .stream()
                                .map(chunk -> SnapshotChunk
                                        .builder()
                                        .name(chunk.getChunkName())
                                        .data(chunk.getData())
                                        .build()
                                )
                                .collect(Collectors.toList())
                );
            }
            RaftUtils.updateIncrementalAtomicLong(context.getCommitIndex(), request.getLastIncludedIndex());
            RaftUtils.updateIncrementalAtomicLong(context.getStateMachineApplyIndex(), request.getLastIncludedIndex());
            context.getOperationLog().shrinkLog(request.getLastIncludedIndex());
            if (!context.getNotifiedStartupSync().get() && context.getNotifiedStartupSync().compareAndSet(false, true)) {
                context.getEventListener().syncedWithLeaderOrSelfIsLeaderOnStartup();
            }
        }

        NettyUtils.writeAndFlushIfChannelActive(
                callbackInfo.getChannel(),
                InstallSnapshotResponse
                        .builder()
                        .groupId(context.getRaftGroupId())
                        .nodeId(context.getSelfNodeId())
                        .term(context.getCurrentTerm().get())
                        .success(true)
                        .lastIndex(context.getOperationLog().getEffectiveLastIndex().get())
                        .build()
        );
    }

    private void processInstallSnapshotResponse(RaftNodeCallbackInfo callbackInfo) {
        InstallSnapshotResponse installSnapshotResponse = (InstallSnapshotResponse) callbackInfo.getMessage();

        if (installSnapshotResponse.getTerm() > context.getCurrentTerm().get()) {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            electionProcessor.processTermGreaterThanCurrent(installSnapshotResponse.getTerm());
            return;
        }

        RaftPeerWrapper wrapper = context.getRaftPeers().get(installSnapshotResponse.getNodeId());
        if (installSnapshotResponse.isSuccess()) {
            RaftUtils.updateIncrementalAtomicLong(wrapper.getNextIndex(), installSnapshotResponse.getLastIndex() + 1);
            RaftUtils.updateIncrementalAtomicLong(wrapper.getMatchIndex(), installSnapshotResponse.getLastIndex());
        }
    }
}
