package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.constants.MDCConstants;
import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.producers.FilesPathsProducer;
import com.lantromipis.configuration.properties.constant.PgFacadeConstants;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.InitializationException;
import com.lantromipis.orchestration.model.PgFacadeRaftNodeInfo;
import com.lantromipis.orchestration.service.api.PgFacadeRaftService;
import com.lantromipis.orchestration.util.RaftUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.NetUtils;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.slf4j.MDC;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
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
    Instance<PlatformAdapter> platformAdapter;

    @Inject
    FilesPathsProducer filesPathsProducer;

    @Inject
    BaseStateMachine stateMachine;

    @Inject
    ManagedExecutor managedExecutor;

    @Inject
    RaftUtils raftUtils;

    private RaftServer raftServer;
    private ThreadGroup raftServerThreadGroup;

    @Override
    public void initialize() throws InitializationException {
        if (OrchestrationProperties.AdapterType.NO_ADAPTER.equals(orchestrationProperties.adapter())) {
            pgFacadeRuntimeProperties.setRaftRole(PgFacadeRaftRole.RAFT_DISABLED);
            return;
        }

        List<PgFacadeRaftNodeInfo> nodeInfos = platformAdapter.get().getActiveRaftNodeInfos()
                .stream()
                .sorted(
                        Comparator.comparing(PgFacadeRaftNodeInfo::getAddress)
                                .thenComparing(PgFacadeRaftNodeInfo::getPort)
                )
                .toList();
        PgFacadeRaftNodeInfo selfNodeInfo = platformAdapter.get().getSelfRaftNodeInfo();

        List<RaftPeer> raftPeers = new ArrayList<>();
        RaftPeer selfPeer = null;
        int selfPort = -1;
        for (int i = 0; i < nodeInfos.size(); i++) {
            PgFacadeRaftNodeInfo nodeInfo = nodeInfos.get(i);
            RaftPeer raftPeer = RaftPeer
                    .newBuilder()
                    .setId(nodeInfo.getPlatformAdapterIdentifier())
                    .setAddress(raftUtils.getRpcAddress(nodeInfo.getAddress(), nodeInfo.getPort()))
                    .setPriority(i)
                    .build();

            // detected self
            if (nodeInfo.getPlatformAdapterIdentifier().equals(selfNodeInfo.getPlatformAdapterIdentifier())) {
                selfPeer = raftPeer;
                selfPort = NetUtils.createSocketAddr(selfPeer.getAddress()).getPort();
            }

            raftPeers.add(raftPeer);
        }

        RaftProperties properties = new RaftProperties();

        // Set the storage directory (different for each peer) in the RaftProperty object
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(new File(filesPathsProducer.getRaftDirPath())));
        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.NETTY);

        // Set the port (different for each peer) in RaftProperty object
        NettyConfigKeys.Server.setPort(properties, selfPort);

        RaftGroup raftGroup = RaftGroup.valueOf(
                RaftGroupId.valueOf(PgFacadeConstants.PG_FACADE_RAFT_GROUP_ID),
                raftPeers
        );

        raftServerThreadGroup = new ThreadGroup("raft-tg");

        try {
            // Build the Raft server
            raftServer = RaftServer.newBuilder()
                    .setGroup(raftGroup)
                    .setProperties(properties)
                    .setServerId(selfPeer.getId())
                    .setStateMachine(stateMachine)
                    .build();


            managedExecutor.runAsync(
                    () -> {
                        try {
                            Thread.sleep(3000);
                            raftServer.start();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
            ).get();

        } catch (Exception e) {
            throw new InitializationException("Error while initializing Raft Server! ", e);
        }

        log.info("Initialized Raft Server. Self peer ID: {}", selfPeer.getId().toString());

        // TODO stub
        pgFacadeRuntimeProperties.setRaftRole(PgFacadeRaftRole.FOLLOWER);
        MDC.put(MDCConstants.RAFT_ROLE, PgFacadeRaftRole.FOLLOWER.getMdcValue());
    }
}
