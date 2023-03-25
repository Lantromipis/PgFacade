package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.producers.FilesPathsProducer;
import com.lantromipis.configuration.properties.constant.PgFacadeConstants;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.InitializationException;
import com.lantromipis.orchestration.exception.RaftException;
import com.lantromipis.orchestration.model.PgFacadeRaftNodeInfo;
import com.lantromipis.orchestration.restclient.HealtcheckTemplateRestClient;
import com.lantromipis.orchestration.restclient.model.HealtcheckResponseDto;
import com.lantromipis.orchestration.service.api.PgFacadeRaftService;
import com.lantromipis.pgfacadeprotocol.model.api.RaftGroup;
import com.lantromipis.pgfacadeprotocol.model.api.RaftNode;
import com.lantromipis.pgfacadeprotocol.model.api.RaftServerProperties;
import com.lantromipis.pgfacadeprotocol.server.api.RaftEventListener;
import com.lantromipis.pgfacadeprotocol.server.api.RaftServer;
import com.lantromipis.pgfacadeprotocol.server.impl.RaftServerImpl;
import io.netty.channel.nio.NioEventLoopGroup;
import io.quarkus.scheduler.Scheduled;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.*;
import org.apache.ratis.rpc.CallId;
import org.checkerframework.checker.units.qual.N;
import org.eclipse.microprofile.rest.client.RestClientBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

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
    RaftEventListener raftEventListener;


    private RaftServer raftServer;

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

        List<RaftNode> raftNodes = nodeInfos.stream()
                .map(info -> RaftNode
                        .builder()
                        .id(info.getPlatformAdapterIdentifier())
                        .groupId(PgFacadeConstants.PG_FACADE_RAFT_GROUP_ID.toString())
                        .port(info.getPort())
                        .ipAddress(info.getAddress())
                        .build()
                )
                .collect(Collectors.toList());

        RaftGroup raftGroup = RaftGroup
                .builder()
                .groupId(PgFacadeConstants.PG_FACADE_RAFT_GROUP_ID.toString())
                .raftNodes(raftNodes)
                .build();

        try {
            raftServer = new RaftServerImpl(
                    new NioEventLoopGroup(),
                    new NioEventLoopGroup(),
                    raftGroup,
                    selfNodeInfo.getPlatformAdapterIdentifier(),
                    new RaftServerProperties(),
                    raftEventListener
            );

            raftServer.start();

            pgFacadeRuntimeProperties.setRaftServerUp(true);

        } catch (Exception e) {
            throw new InitializationException("Error while initializing Raft Server! ", e);
        }

        log.info("Initialized Raft Server. Self peer ID: {}", selfNodeInfo.getPlatformAdapterIdentifier());
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

            awaitNewRaftNodeReadiness(newNodeInfo.getAddress());

            log.info("Adding new raft peer with ID {}", newNodeInfo.getPlatformAdapterIdentifier());

            raftServer.addNewNode(raftNode);

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

            for (int i = 0; i < 500; i++) {
                try {
                    HealtcheckResponseDto response = healtcheckRestClient.checkReadiness();
                    boolean raftReady = response.getChecks()
                            .stream()
                            .anyMatch(healthcheckItem ->
                                    healthcheckItem.getName().equals(PgFacadeConstants.RAFT_SERVER_UP_READINESS_CHECK)
                                            && healthcheckItem.getStatus().equals(HealtcheckResponseDto.HealtcheckStatus.UP)
                            );
                    if (raftReady) {
                        log.info("New raft peer server is ready!");
                        return;
                    }
                } catch (Exception ignored) {
                }

                Thread.sleep(10);
            }

            throw new RaftException("Timout reached for new PgFacade raft server to become ready.");

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
