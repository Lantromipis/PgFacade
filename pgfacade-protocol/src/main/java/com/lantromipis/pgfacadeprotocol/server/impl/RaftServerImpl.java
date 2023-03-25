package com.lantromipis.pgfacadeprotocol.server.impl;

import com.lantromipis.pgfacadeprotocol.message.*;
import com.lantromipis.pgfacadeprotocol.model.api.RaftGroup;
import com.lantromipis.pgfacadeprotocol.model.api.RaftNode;
import com.lantromipis.pgfacadeprotocol.model.api.RaftRole;
import com.lantromipis.pgfacadeprotocol.model.api.RaftServerProperties;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftNodeCallbackInfo;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftPeerWrapper;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftServerContext;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftServerOperationsLog;
import com.lantromipis.pgfacadeprotocol.netty.RaftServerChannelInitializer;
import com.lantromipis.pgfacadeprotocol.server.api.RaftEventListener;
import com.lantromipis.pgfacadeprotocol.server.api.RaftServer;
import com.lantromipis.pgfacadeprotocol.server.internal.RaftElectionProcessor;
import com.lantromipis.pgfacadeprotocol.utils.NettyUtils;
import com.lantromipis.pgfacadeprotocol.utils.RaftUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CopyOnWriteArrayList;
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

    public RaftServerImpl(EventLoopGroup bossGroup, EventLoopGroup workerGroup, RaftGroup raftGroup, String selfRaftNodeId, RaftServerProperties raftServerProperties, RaftEventListener raftEventListener) {
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
        context.setRaftEventListener(raftEventListener);
        context.setRaftServerOperationsLog(new RaftServerOperationsLog());
        context.setVoteResponses(new CopyOnWriteArrayList<>());

        electionProcessor = new RaftElectionProcessor(
                context,
                properties,
                this::processRaftNodeResponseCallback
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
    }

    @Override
    public void addNewNode(RaftNode raftNode) {
        context.getRaftPeers()
                .put(
                        raftNode.getGroupId(),
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

    public void shutdown() throws InterruptedException {
        voteTimer.stop();
        heartbeatTimer.stop();
        serverChannelFuture.channel().close().sync();
    }

    private void heartBeat() {
        AppendRequest appendRequest = AppendRequest.builder()
                .groupId(context.getRaftGroupId())
                .nodeId(context.getSelfNodeId())
                .currentTerm(context.getCurrentTerm().get())
                .leaderCommit(context.getCommitIndex().get())
                .build();

        RaftUtils.tryToSendMessageToAll(
                appendRequest,
                context,
                properties,
                this::processRaftNodeResponseCallback
        );
    }

    private void processRaftNodeResponseCallback(RaftNodeCallbackInfo callbackInfo) {
        AbstractMessage message = callbackInfo.getMessage();

        // security
        if (!message.getGroupId().equals(context.getRaftGroupId()) || !context.getRaftPeers().containsKey(message.getNodeId())) {
            log.debug("Rejecting request. Node with group id '{}' and node id '{}' is not in current conf.", message.getGroupId(), message.getNodeId());

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
                log.debug("Received VOTE RESPONSE from node {}", message.getNodeId());
                context.getVoteResponses().add((VoteResponse) callbackInfo.getMessage());
            }
            case APPEND_RESPONSE_MESSAGE_MARKER -> {
                log.debug("Received APPEND RESPONSE from node {}", message.getNodeId());
                AppendResponse appendResponse = (AppendResponse) message;

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
                // commit?
            }
        }
    }

    private void processRaftNodeRequestCallback(RaftNodeCallbackInfo callbackInfo) {
        AbstractMessage message = callbackInfo.getMessage();

        // security
        if (!message.getGroupId().equals(context.getRaftGroupId()) || !context.getRaftPeers().containsKey(message.getNodeId())) {
            log.debug("Rejecting request. Node with group id '{}' and node id '{}' is not in current conf.", message.getGroupId(), message.getNodeId());

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
                log.debug("Received VOTE REQUEST from node {}", message.getNodeId());

                NettyUtils.writeAndFlushIfChannelActive(
                        callbackInfo.getChannel(),
                        electionProcessor.vote((VoteRequest) message)
                );
            }
            case APPEND_REQUEST_MESSAGE_MARKER -> {
                log.debug("Received APPEND REQUEST from node {}", message.getNodeId());

                AppendRequest appendRequest = (AppendRequest) message;

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
                if (appendRequest.getPreviousLogIndex() > context.getRaftServerOperationsLog().getLastIndex().get()
                        || appendRequest.getPreviousTerm() != context.getRaftServerOperationsLog().getTerm(appendRequest.getPreviousLogIndex())) {

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

                // TODO
            }
        }
    }
}
