package com.lantromipis.pgfacadeprotocol.server.impl;

import com.lantromipis.pgfacadeprotocol.constant.InternalRaftCommandsConstants;
import com.lantromipis.pgfacadeprotocol.exception.NotActiveException;
import com.lantromipis.pgfacadeprotocol.exception.NotLeaderException;
import com.lantromipis.pgfacadeprotocol.message.*;
import com.lantromipis.pgfacadeprotocol.model.api.RaftGroup;
import com.lantromipis.pgfacadeprotocol.model.api.RaftNode;
import com.lantromipis.pgfacadeprotocol.model.api.RaftRole;
import com.lantromipis.pgfacadeprotocol.model.api.RaftServerProperties;
import com.lantromipis.pgfacadeprotocol.model.internal.*;
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
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.lantromipis.pgfacadeprotocol.constant.MessageMarkerConstants.*;

@Slf4j
public class RaftServerImpl implements RaftServer {

    private RaftServerProperties properties;
    private RaftServerContext context;

    private RaftTimer heartbeatTimer;
    private RaftTimer voteTimer;

    private ChannelFuture serverChannelFuture;
    private RaftElectionProcessor electionProcessor;
    private RaftCommitProcessor commitProcessor;

    public RaftServerImpl(EventLoopGroup bossGroup, EventLoopGroup workerGroup, RaftGroup raftGroup, String selfRaftNodeId, RaftServerProperties raftServerProperties, RaftEventListener raftEventListener, RaftStateMachine raftStateMachine) {
        properties = raftServerProperties;

        context = new RaftServerContext();
        context.setBossGroup(bossGroup);
        context.setWorkerGroup(workerGroup);
        context.setRaftGroupId(raftGroup.getGroupId());

        context.setRaftPeers(
                raftGroup.getRaftNodes()
                        .stream()
                        .filter(raftNode -> !raftNode.getId().equals(selfRaftNodeId))
                        .collect(
                                Collectors.toMap(
                                        RaftNode::getId,
                                        item -> new RaftPeerWrapper(
                                                RaftNode.builder()
                                                        .id(item.getId())
                                                        .groupId(raftGroup.getGroupId())
                                                        .ipAddress(item.getIpAddress())
                                                        .port(item.getPort())
                                                        .build()
                                        )
                                )
                        )
        );

        context.setSelfNodeId(selfRaftNodeId);
        context.setSelfRole(RaftRole.FOLLOWER);
        context.setCurrentTerm(new AtomicLong(0));
        context.setCommitIndex(new AtomicLong(-1));
        context.setEventListener(raftEventListener);
        context.setRaftStateMachine(raftStateMachine);
        context.setOperationLog(new RaftServerOperationsLog());
        context.setVoteResponses(new CopyOnWriteArrayList<>());
        context.setActive(false);
        context.setCommitInProgress(new AtomicBoolean(false));

        electionProcessor = new RaftElectionProcessor(
                context,
                properties,
                this::processRaftNodeResponseCallback
        );
        commitProcessor = new RaftCommitProcessor(
                context
        );
    }

    @Override
    public void start() throws InterruptedException {
        ServerBootstrap serverBootstrap = new ServerBootstrap();

        serverChannelFuture = serverBootstrap
                .group(context.getBossGroup(), context.getWorkerGroup())
                .channel(NioServerSocketChannel.class)
                .childHandler(new RaftServerChannelInitializer(this::processRaftNodeRequestCallback))
                .childOption(ChannelOption.AUTO_READ, false)
                .bind(properties.getPort())
                .sync();


        heartbeatTimer = new RaftTimer(
                0,
                properties.getHeartbeatTimeout(),
                this::heartBeat,
                () -> context.getSelfRole().equals(RaftRole.LEADER)
        );
        voteTimer = new RaftTimer(
                properties.getStartupLeaderHeartbeatAwait(),
                properties.getVoteTimeout(),
                () -> electionProcessor.processElection(),
                () -> context.getSelfRole().equals(RaftRole.FOLLOWER)
        );

        voteTimer.start();
        heartbeatTimer.start();
        context.setActive(true);
    }

    @Override
    public void addNewNode(RaftNode raftNode) throws NotLeaderException, NotActiveException {
        RaftNode newRaftNode = RaftNode.builder()
                .id(raftNode.getId())
                .groupId(context.getRaftGroupId())
                .ipAddress(raftNode.getIpAddress())
                .port(raftNode.getPort())
                .build();

        appendToLog(
                InternalRaftCommandsConstants.ADD_NEW_RAFT_NODE_COMMAND,
                InternalCommandsEncoderDecoder.encodeAddRaftNodeCommandData(newRaftNode)
        );

        commitProcessor.leaderCommit();
    }

    @Override
    public void shutdown() throws InterruptedException {
        context.setActive(false);
        voteTimer.stop();
        heartbeatTimer.stop();
        serverChannelFuture.channel().close().sync();
    }

    @Override
    public long appendToLog(String command, byte[] data) throws NotLeaderException, NotActiveException {
        if (!context.getSelfRole().equals(RaftRole.LEADER)) {
            throw new NotLeaderException();
        }

        if (!context.isActive()) {
            throw new NotActiveException();
        }

        long index = context.getOperationLog().append(
                context.getCurrentTerm().get(),
                command,
                data
        );

        heartBeat();

        return index;
    }

    private void heartBeat() {
        for (var wrapper : context.getRaftPeers().values()) {
            List<AppendRequest.Operation> operations = new ArrayList<>();
            long nextPeerIndex = wrapper.getNextIndex().get();
            long prevPeerIndex = nextPeerIndex - 1;
            long lastIndex = context.getOperationLog().getLastIndex().get();

            if (nextPeerIndex <= lastIndex) {
                LogEntry logEntry = context.getOperationLog().getLogEntry(nextPeerIndex);
                // TODO only single operation for now for stability
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
            // else prevPeerIndex = nextPeerIndex

            AppendRequest appendRequest = AppendRequest.builder()
                    .groupId(context.getRaftGroupId())
                    .nodeId(context.getSelfNodeId())
                    .previousLogIndex(prevPeerIndex)
                    .previousTerm(context.getOperationLog().getTerm(prevPeerIndex))
                    .currentTerm(context.getCurrentTerm().get())
                    .leaderCommit(context.getCommitIndex().get())
                    .operations(operations)
                    .build();

            RaftUtils.tryToSendMessageToPeer(
                    wrapper,
                    appendRequest,
                    context,
                    properties,
                    this::processRaftNodeResponseCallback
            );
        }
    }

    private void processRaftNodeResponseCallback(RaftNodeCallbackInfo callbackInfo) {
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

        switch (message.getMessageMarker()) {
            case VOTE_RESPONSE_MESSAGE_MARKER -> {
                //log.trace("Received VOTE RESPONSE from node {}", message.getNodeId());
                context.getVoteResponses().add((VoteResponse) callbackInfo.getMessage());
            }
            case APPEND_RESPONSE_MESSAGE_MARKER -> {
                //log.trace("Received APPEND RESPONSE from node {}", message.getNodeId());
                processAppendResponse(callbackInfo);
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
            wrapper.getMatchIndex().set(appendResponse.getMatchIndex());
        } else {
            // failed, decrement and retry
            wrapper.getNextIndex().decrementAndGet();
        }

        commitProcessor.leaderCommit();
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
                            .build()
            );
            return;
        }

        voteTimer.reset();

        if (appendRequest.getCurrentTerm() > context.getCurrentTerm().get()) {
            //If RPC request or response contains term T > currentTerm: set currentTerm = T
            electionProcessor.processTermGreaterThanCurrent(appendRequest.getCurrentTerm());
        }

        if (!context.getSelfRole().equals(RaftRole.FOLLOWER)) {
            context.setSelfRole(RaftRole.FOLLOWER);
        }

        // Reply false if operations does not contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
        if (appendRequest.getPreviousLogIndex() > context.getOperationLog().getLastIndex().get()
                || appendRequest.getPreviousTerm() != context.getOperationLog().getTerm(appendRequest.getPreviousLogIndex())) {
            NettyUtils.writeAndFlushIfChannelActive(
                    callbackInfo.getChannel(),
                    AppendResponse
                            .builder()
                            .groupId(context.getRaftGroupId())
                            .nodeId(context.getSelfNodeId())
                            .term(context.getCurrentTerm().get())
                            .success(false)
                            .matchIndex(-1)
                            .build()
            );
            return;
        }

        var operationLog = context.getOperationLog();

        if (CollectionUtils.isNotEmpty(appendRequest.getOperations())) {
            // TODO only one operation for now for stability
            AppendRequest.Operation operation = appendRequest.getOperations().get(0);
            long newOperationIndex = appendRequest.getPreviousLogIndex() + 1;
            // If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)

            if (newOperationIndex <= operationLog.getLastIndex().get()
                    && operation.getTerm() != operationLog.getTerm(newOperationIndex)) {
                operationLog.removeAllStartingFrom(newOperationIndex);
            }

            // Append any new entries not already in the operations
            if (newOperationIndex > operationLog.getLastIndex().get()) {
                long index = operationLog.append(
                        operation.getTerm(),
                        operation.getCommand(),
                        operation.getData()
                );
                //log.debug("Appended entry with index {} command {} and data {} to log!", index, operation.getCommand(), new String(operation.getData()));
            }
        }

        if (appendRequest.getLeaderCommit() > context.getCommitIndex().get()) {
            commitProcessor.followerCommit(appendRequest.getLeaderCommit());
        }

        NettyUtils.writeAndFlushIfChannelActive(
                callbackInfo.getChannel(),
                AppendResponse
                        .builder()
                        .groupId(context.getRaftGroupId())
                        .nodeId(context.getSelfNodeId())
                        .term(context.getCurrentTerm().get())
                        .success(true)
                        .matchIndex(operationLog.getLastIndex().get())
                        .build()
        );
    }
}
