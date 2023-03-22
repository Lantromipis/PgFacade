package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.producers.FilesPathsProducer;
import com.lantromipis.configuration.properties.constant.PgFacadeConstants;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.InitializationException;
import com.lantromipis.orchestration.exception.RaftException;
import com.lantromipis.orchestration.mapper.RaftMapper;
import com.lantromipis.orchestration.model.PgFacadeRaftNodeInfo;
import com.lantromipis.orchestration.restclient.HealtcheckTemplateRestClient;
import com.lantromipis.orchestration.restclient.model.HealtcheckResponseDto;
import com.lantromipis.orchestration.service.api.PgFacadeRaftService;
import com.lantromipis.orchestration.util.RaftUtils;
import io.quarkus.scheduler.Scheduled;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.rpc.CallId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.NetUtils;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.eclipse.microprofile.rest.client.RestClientBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.io.File;
import java.net.URI;
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
    RaftMapper raftMapper;

    @Inject
    RaftUtils raftUtils;

    private RaftServer raftServer;
    private ThreadGroup raftServerThreadGroup;
    private RaftPeer selfRaftPeer = null;

    @Override
    public void initialize() throws InitializationException {
        pgFacadeRuntimeProperties.setRaftRole(PgFacadeRaftRole.FOLLOWER);

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
        int selfPort = -1;
        for (int i = 0; i < nodeInfos.size(); i++) {
            PgFacadeRaftNodeInfo nodeInfo = nodeInfos.get(i);
            RaftPeer raftPeer = raftMapper.from(nodeInfo);

            // detected self
            if (nodeInfo.getPlatformAdapterIdentifier().equals(selfNodeInfo.getPlatformAdapterIdentifier())) {
                selfRaftPeer = raftPeer;
                selfPort = NetUtils.createSocketAddr(selfRaftPeer.getAddress()).getPort();
            }

            raftPeers.add(raftPeer);
        }

        RaftProperties properties = new RaftProperties();

        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(new File(filesPathsProducer.getRaftDirPath())));
        //RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.NETTY);

        // Set the port (different for each peer) in RaftProperty object
        //NettyConfigKeys.Server.setPort(properties, selfPort);
        GrpcConfigKeys.Server.setPort(properties, selfPort);

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
                    .setServerId(selfRaftPeer.getId())
                    .setStateMachine(stateMachine)
                    .build();


            managedExecutor.runAsync(
                    () -> {
                        try {
                            raftServer.start();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
            ).get();

            pgFacadeRuntimeProperties.setRaftServerUp(true);

        } catch (Exception e) {
            throw new InitializationException("Error while initializing Raft Server! ", e);
        }

        log.info("Initialized Raft Server. Self peer ID: {}", selfRaftPeer.getId().toString());
    }

    @Override
    public void addNewRaftNode(PgFacadeRaftNodeInfo newNodeInfo) throws RaftException {
        try {
            RaftGroupId raftGroupId = RaftGroupId.valueOf(PgFacadeConstants.PG_FACADE_RAFT_GROUP_ID);
            RaftPeer newPeer = raftMapper.from(newNodeInfo);

            awaitNewRaftNodeReadiness(newNodeInfo.getAddress());

            log.info("Adding new raft peer with ID {}", newPeer.getId());

            RaftClientReply raftClientReply = raftServer.setConfiguration(new SetConfigurationRequest(
                            ClientId.randomId(),
                            selfRaftPeer.getId(),
                            raftGroupId,
                            CallId.getDefault(),
                            SetConfigurationRequest.Arguments
                                    .newBuilder()
                                    .setServersInNewConf(List.of(newPeer))
                                    .setMode(SetConfigurationRequest.Mode.ADD)
                                    .build()
                    )
            );

            if (raftClientReply.isSuccess()) {
                log.info("Successfully added new raft peer with ID {}", newPeer.getId());
            } else {
                throw new RaftException("Failed to add new raft peer! ");
            }

        } catch (RaftException e) {
            platformAdapter.get().deleteInstance(newNodeInfo.getPlatformAdapterIdentifier());
            throw e;
        } catch (Exception e) {
            platformAdapter.get().deleteInstance(newNodeInfo.getPlatformAdapterIdentifier());
            throw new RaftException("Failed to add new raft peer! ", e);
        }
    }

    private void awaitNewRaftNodeReadiness(String address) {
        URI anyDynamicUrl = URI.create("http://" + address + ":8080");
        try (HealtcheckTemplateRestClient healtcheckRestClient = RestClientBuilder.newBuilder()
                .baseUri(anyDynamicUrl)
                .build(HealtcheckTemplateRestClient.class)) {

            for (int i = 0; i < 200; i++) {
                try {
                    HealtcheckResponseDto response = healtcheckRestClient.checkLiveliness();
                    boolean raftReady = response.getChecks()
                            .stream()
                            .anyMatch(healthcheckItem ->
                                    healthcheckItem.getName().equals(PgFacadeConstants.RAFT_SERVER_UP_READINESS_CHECK)
                                            && healthcheckItem.getStatus().equals(HealtcheckResponseDto.HealtcheckStatus.UP)
                            );
                    if (raftReady) {
                        log.info("RAFT READY LOL");
                        return;
                    }
                    log.info("RAFT NOT READY");
                } catch (Exception ignored) {
                }

                Thread.sleep(10);
            }

        } catch (RaftException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RaftException("Failed to add new raft peer! ", e);
        } catch (Exception e) {
            throw new RaftException("Failed to add new raft peer! ", e);
        }
    }

    @Scheduled(every = "PT3S")
    public void test() {
        int a = 0;
        int b = a + 1;
    }
}
