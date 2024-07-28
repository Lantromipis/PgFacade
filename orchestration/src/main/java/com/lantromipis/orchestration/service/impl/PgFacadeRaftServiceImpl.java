package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.properties.constant.PgFacadeConstants;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.predefined.RaftProperties;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.PgFacadePlatformAdapter;
import com.lantromipis.orchestration.exception.InitializationException;
import com.lantromipis.orchestration.exception.RaftException;
import com.lantromipis.orchestration.model.PgFacadeRaftNodeInfo;
import com.lantromipis.orchestration.service.api.PgFacadeRaftService;
import com.lantromipis.orchestration.service.api.raft.PgFacadeRaftStateMachine;
import com.lantromipis.pgfacadeprotocol.model.api.*;
import com.lantromipis.pgfacadeprotocol.server.api.RaftEventListener;
import com.lantromipis.pgfacadeprotocol.server.api.RaftServer;
import com.lantromipis.pgfacadeprotocol.server.impl.RaftServerImpl;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;

@Slf4j
@ApplicationScoped
public class PgFacadeRaftServiceImpl implements PgFacadeRaftService {

    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    @Inject
    OrchestrationProperties orchestrationProperties;

    @Inject
    Instance<PgFacadePlatformAdapter> platformAdapter;

    @Inject
    RaftEventListener raftEventListener;

    @Inject
    PgFacadeRaftStateMachine raftStateMachine;

    @Inject
    RaftProperties raftProperties;

    private RaftServer raftServer;

    @Override
    public void initialize() throws InitializationException {
        pgFacadeRuntimeProperties.setRaftRole(PgFacadeRaftRole.FOLLOWER);

        if (OrchestrationProperties.AdapterType.NO_ADAPTER.equals(orchestrationProperties.adapter())) {
            pgFacadeRuntimeProperties.setRaftRole(PgFacadeRaftRole.RAFT_DISABLED);
            return;
        }

        PgFacadeRaftNodeInfo selfNodeInfo = platformAdapter.get().getSelfRaftNodeInfo();
        List<RaftNode> raftNodes = platformAdapter.get().getActiveRaftNodeInfos()
                .stream()
                .sorted(
                        Comparator.comparing(PgFacadeRaftNodeInfo::getAddress)
                                .thenComparing(PgFacadeRaftNodeInfo::getPort)
                )
                .map(info -> RaftNode
                        .builder()
                        .id(info.getPlatformAdapterIdentifier())
                        .groupId(PgFacadeConstants.PG_FACADE_RAFT_GROUP_ID.toString())
                        .port(info.getPort())
                        .ipAddress(info.getAddress())
                        .build()
                )
                .toList();

        RaftGroup raftGroup = RaftGroup
                .builder()
                .groupId(PgFacadeConstants.PG_FACADE_RAFT_GROUP_ID.toString())
                .raftNodes(raftNodes)
                .build();

        try {
            raftServer = new RaftServerImpl(
                    Epoll.isAvailable() ? new EpollEventLoopGroup(1) : new NioEventLoopGroup(1),
                    Epoll.isAvailable() ? new EpollEventLoopGroup(raftProperties.serverWorkThreads()) : new NioEventLoopGroup(raftProperties.serverWorkThreads()),
                    raftGroup,
                    selfNodeInfo.getPlatformAdapterIdentifier(),
                    new RaftServerProperties(),
                    raftEventListener,
                    raftStateMachine
            );

            log.info("Starting raft server and waiting for it to be synced with leader...");
            raftServer.start();

            pgFacadeRuntimeProperties.setRaftServerUp(true);

            log.info("Raft server is synced with leader!");

        } catch (Exception e) {
            throw new InitializationException("Error while initializing Raft Server! ", e);
        }

        log.info("Initialized Raft Server. Self peer ID: {}", selfNodeInfo.getPlatformAdapterIdentifier());
    }

    @Override
    public void shutdown(boolean simulateRoleChangedToFollower) {
        if (raftServer == null) {
            log.info("Raft server stopped.");
            return;
        }
        try {
            log.info("Stopping Raft server...");
            raftServer.shutdown();
            if (simulateRoleChangedToFollower) {
                raftEventListener.selfRoleChanged(RaftRole.FOLLOWER);
            }
            log.info("Raft server stopped.");
        } catch (Exception e) {
            log.error("Error during Raft server shutdown", e);
        }
    }

    @Override
    public void addNewRaftNode(PgFacadeRaftNodeInfo newNodeInfo) throws RaftException {
        try {
            RaftNode raftNode = RaftNode
                    .builder()
                    .id(newNodeInfo.getPlatformAdapterIdentifier())
                    .groupId(PgFacadeConstants.PG_FACADE_RAFT_GROUP_ID.toString())
                    .port(newNodeInfo.getPort())
                    .ipAddress(newNodeInfo.getAddress())
                    .build();

            log.info("Adding new raft peer with ID {}", newNodeInfo.getPlatformAdapterIdentifier());

            raftServer.addNewNode(raftNode);

        } catch (RaftException e) {
            throw e;
        } catch (Exception e) {
            throw new RaftException("Failed to add new raft peer! ", e);
        }
    }

    public void removeNode(String nodeAdapterIdentifier) throws RaftException {
        try {
            log.info("Removing raft peer with ID {}", nodeAdapterIdentifier);
            raftServer.removeNode(nodeAdapterIdentifier);
        } catch (RaftException e) {
            throw e;
        } catch (Exception e) {
            throw new RaftException("Failed to remove raft peer! ", e);
        }
    }

    @Override
    public List<RaftPeerInfo> getRaftPeersFromServer() throws RaftException {
        return raftServer.getRaftPeers();
    }

    @Override
    public String getSelfRaftNodeId() {
        return raftServer.getSelfRaftNodeId();
    }

    @Override
    public void appendToLogAndAwaitCommit(String command, byte[] data, long timeoutMs) throws RaftException {
        long commitIdx = 0;
        try {
            commitIdx = raftServer.appendToLog(
                    command,
                    data
            );
            log.debug("Appended command {} with index {}", command, commitIdx);
            raftStateMachine.awaitCommit(commitIdx, timeoutMs);
        } catch (RaftException e) {
            log.debug("Error while waiting for command {} with index {} to commit", command, commitIdx);
            throw e;
        } catch (InterruptedException e) {
            log.debug("Error while waiting for command {} with index {} to commit", command, commitIdx);
            Thread.currentThread().interrupt();
            throw new RaftException("Error while waiting for commit to complete.", e);
        } catch (Exception e) {
            log.debug("Error while waiting for command {} with index {} to commit", command, commitIdx);
            throw new RaftException("Error while waiting for commit to complete.", e);
        }
    }
}
