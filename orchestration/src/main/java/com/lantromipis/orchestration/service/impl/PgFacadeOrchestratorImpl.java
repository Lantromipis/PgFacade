package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.properties.constant.PgFacadeConstants;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.predefined.RaftProperties;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.RaftException;
import com.lantromipis.orchestration.model.ExternalLoadBalancerAdapterInfo;
import com.lantromipis.orchestration.model.PgFacadeRaftNodeInfo;
import com.lantromipis.orchestration.model.raft.ExternalLoadBalancerRaftInfo;
import com.lantromipis.orchestration.restclient.ExternalLoadBalancerHealtcheckTemplateRestClient;
import com.lantromipis.orchestration.restclient.PgFacadeHealtcheckTemplateRestClient;
import com.lantromipis.orchestration.restclient.model.HealtcheckResponseDto;
import com.lantromipis.orchestration.service.api.PgFacadeOrchestrator;
import com.lantromipis.orchestration.service.api.PgFacadeRaftService;
import com.lantromipis.orchestration.util.RaftFunctionalityCombinator;
import com.lantromipis.pgfacadeprotocol.model.api.RaftPeerInfo;
import io.quarkus.scheduler.Scheduled;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.rest.client.RestClientBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.io.Closeable;
import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@ApplicationScoped
public class PgFacadeOrchestratorImpl implements PgFacadeOrchestrator {
    @Inject
    Instance<PlatformAdapter> platformAdapter;

    @Inject
    PgFacadeRaftService pgFacadeRaftService;

    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    @Inject
    RaftProperties raftProperties;

    @Inject
    RaftFunctionalityCombinator raftFunctionalityCombinator;

    @Inject
    OrchestrationProperties orchestrationProperties;

    private ConcurrentHashMap<String, PgFacadeInstanceStateInfo> pgFacadeInstances = new ConcurrentHashMap<>();
    private PgFacadeLoadBalancerStateInfo loadBalancerStateInfo;

    @Override
    public void startOrchestration() {
        log.info("Starting PgFacade orchestration!");
    }

    @Scheduled(every = "${pg-facade.orchestration.common.external-load-balancer.healthcheck-interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void checkExternalLoadBalancerHealth() {
        if (PgFacadeRaftRole.LEADER.equals(pgFacadeRuntimeProperties.getRaftRole()) && orchestrationProperties.common().externalLoadBalancer().deploy()) {
            try {
                ExternalLoadBalancerRaftInfo info = raftFunctionalityCombinator.getPgFacadeLoadBalancerInfo();

                if (info != null) {
                    if (loadBalancerStateInfo == null || !loadBalancerStateInfo.getAdapterIdentifier().equals(info.getAdapterIdentifier())) {
                        if (loadBalancerStateInfo != null) {
                            platformAdapter.get().deleteInstance(loadBalancerStateInfo.getAdapterIdentifier());
                            closeClient(loadBalancerStateInfo.getHealtcheckClient());
                        }
                        loadBalancerStateInfo = raftLoadBalancerInfoToState(info);
                    }
                } else {
                    if (loadBalancerStateInfo != null) {
                        platformAdapter.get().deleteInstance(loadBalancerStateInfo.getAdapterIdentifier());
                        closeClient(loadBalancerStateInfo.getHealtcheckClient());
                    }
                    loadBalancerStateInfo = null;
                    log.info("No information about deployed load balancer found.");
                }

                if (loadBalancerStateInfo != null) {
                    if (loadBalancerStateInfo.getCreatedWhen().isAfter(Instant.now().minus(orchestrationProperties.common().externalLoadBalancer().healthcheckAwait()))) {
                        return;
                    }
                    HealtcheckResponseDto healtcheckResponseDto = loadBalancerStateInfo.getHealtcheckClient().checkLiveliness();
                    if (HealtcheckResponseDto.HealtcheckStatus.UP.equals(healtcheckResponseDto.getStatus())) {
                        return;
                    } else {
                        log.error("External load balancer unhealthy! Will force redeploy it.");
                        platformAdapter.get().deleteInstance(loadBalancerStateInfo.getAdapterIdentifier());
                        closeClient(loadBalancerStateInfo.getHealtcheckClient());
                        loadBalancerStateInfo = null;
                    }
                }
            } catch (Exception e) {
                log.error("Failed to check load balancer health. Will redeploy it...", e);
            }

            log.info("Redeploying external load balancer...");
            String loadBalancerAdapterIdentifier = null;
            try {
                ExternalLoadBalancerAdapterInfo loadBalancerAdapterInfo = platformAdapter.get().createAndStartExternalLoadBalancerInstance();
                loadBalancerAdapterIdentifier = loadBalancerAdapterInfo.getAdapterIdentifier();

                ExternalLoadBalancerRaftInfo newRaftInfo = ExternalLoadBalancerRaftInfo
                        .builder()
                        .adapterIdentifier(loadBalancerAdapterInfo.getAdapterIdentifier())
                        .address(loadBalancerAdapterInfo.getAddress())
                        .port(loadBalancerAdapterInfo.getHttpPort())
                        .build();
                raftFunctionalityCombinator.savePgFacadeLoadBalancerInfo(newRaftInfo);
                loadBalancerStateInfo = raftLoadBalancerInfoToState(newRaftInfo);
                log.info("External load balancer deployed!");
            } catch (Exception e) {
                log.error("Failed to deploy external load balancer!", e);
                if (loadBalancerAdapterIdentifier != null) {
                    platformAdapter.get().deleteInstance(loadBalancerAdapterIdentifier);
                }
            }

        }
    }

    @Scheduled(every = "${pg-facade.raft.nodes-check-interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void checkStandbyCount() {
        if (PgFacadeRaftRole.LEADER.equals(pgFacadeRuntimeProperties.getRaftRole())) {
            Map<String, RaftPeerInfo> raftPeerInfos = pgFacadeRaftService.getRaftPeersFromServer()
                    .stream()
                    .collect(
                            Collectors.toMap(
                                    RaftPeerInfo::getId,
                                    Function.identity()
                            )
                    );

            for (var raftPeer : raftPeerInfos.values()) {
                if (!pgFacadeInstances.containsKey(raftPeer.getId())) {
                    pgFacadeInstances.put(
                            raftPeer.getId(),
                            PgFacadeInstanceStateInfo
                                    .builder()
                                    .raftIdAndAdapterId(raftPeer.getId())
                                    .client(createHealtcheckRestClient(PgFacadeHealtcheckTemplateRestClient.class, raftPeer.getIpAddress(), pgFacadeRuntimeProperties.getHttpPort()))
                                    .unsuccessfulHealtcheckCount(new AtomicInteger(0))
                                    .build()
                    );
                }
            }

            for (var instance : pgFacadeInstances.values()) {
                try {
                    RaftPeerInfo peerInfo = raftPeerInfos.get(instance.getRaftIdAndAdapterId());
                    if (raftPeerInfos != null
                            && Instant.now().compareTo(Instant.ofEpochMilli(peerInfo.getLastTimeActive()).plus(raftProperties.raftNoResponseTimeoutBeforeKill())) > 0) {
                        killUnhealthyNode(peerInfo.getId());
                        continue;
                    }

                    HealtcheckResponseDto response = instance.getClient().checkLiveliness();
                    if (!HealtcheckResponseDto.HealtcheckStatus.UP.equals(response.getStatus())) {
                        if (instance.getUnsuccessfulHealtcheckCount().incrementAndGet() > raftProperties.appChecksRetryBeforeKill()) {
                            killUnhealthyNode(instance.getRaftIdAndAdapterId());
                        }
                    } else {
                        instance.getUnsuccessfulHealtcheckCount().set(0);
                    }
                } catch (Exception e) {
                    if (instance.getUnsuccessfulHealtcheckCount().incrementAndGet() > raftProperties.appChecksRetryBeforeKill()) {
                        killUnhealthyNode(instance.getRaftIdAndAdapterId());
                    }
                }
            }

            if (platformAdapter.get().getActiveRaftNodeInfos().size() < raftProperties.nodesCount()) {
                PgFacadeRaftNodeInfo raftNodeInfo = null;
                try {
                    raftNodeInfo = platformAdapter.get().createAndStartNewPgFacadeInstance();

                    awaitNewRaftNodeReadiness(raftNodeInfo.getAddress());

                    pgFacadeRaftService.addNewRaftNode(raftNodeInfo);
                    pgFacadeInstances.put(
                            raftNodeInfo.getPlatformAdapterIdentifier(),
                            PgFacadeInstanceStateInfo
                                    .builder()
                                    .raftIdAndAdapterId(raftNodeInfo.getPlatformAdapterIdentifier())
                                    .client(createHealtcheckRestClient(PgFacadeHealtcheckTemplateRestClient.class, raftNodeInfo.getAddress(), pgFacadeRuntimeProperties.getHttpPort()))
                                    .unsuccessfulHealtcheckCount(new AtomicInteger(0))
                                    .build()
                    );
                } catch (Exception e) {
                    if (raftNodeInfo != null) {
                        platformAdapter.get().deleteInstance(raftNodeInfo.getPlatformAdapterIdentifier());
                    }
                    log.error("Failed to create and start new PgFacade instance!", e);
                }
            }
        }
    }

    private void killUnhealthyNode(String id) {
        log.info("PgFacade node with id {} was unhealthy. Removing it...", id);
        Optional.ofNullable(pgFacadeInstances.get(id))
                .map(PgFacadeInstanceStateInfo::getClient)
                .ifPresent(this::closeClient);
        pgFacadeRaftService.removeNode(id);
        pgFacadeInstances.remove(id);
        platformAdapter.get().deleteInstance(id);
    }

    private <T> T createHealtcheckRestClient(Class<T> clazz, String address, int port) {
        URI uri = URI.create("http://" + address + ":" + port);

        return RestClientBuilder.newBuilder()
                .baseUri(uri)
                .connectTimeout(1000, TimeUnit.MILLISECONDS)
                .readTimeout(1000, TimeUnit.MILLISECONDS)
                .build(clazz);
    }

    private void closeClient(Closeable client) {
        try {
            client.close();
        } catch (Exception ignored) {
        }
    }

    private void awaitNewRaftNodeReadiness(String address) throws RaftException {
        try (PgFacadeHealtcheckTemplateRestClient healtcheckRestClient = createHealtcheckRestClient(PgFacadeHealtcheckTemplateRestClient.class, address, pgFacadeRuntimeProperties.getHttpPort())) {
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

    private PgFacadeLoadBalancerStateInfo raftLoadBalancerInfoToState(ExternalLoadBalancerRaftInfo raftInfo) {
        return PgFacadeLoadBalancerStateInfo
                .builder()
                .healtcheckClient(
                        createHealtcheckRestClient(
                                ExternalLoadBalancerHealtcheckTemplateRestClient.class,
                                raftInfo.getAddress(),
                                raftInfo.getPort()
                        )
                )
                .adapterIdentifier(raftInfo.getAdapterIdentifier())
                .createdWhen(Instant.now())
                .build();
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    private static class PgFacadeInstanceStateInfo {
        private String raftIdAndAdapterId;
        private String address;
        private AtomicInteger unsuccessfulHealtcheckCount;
        private PgFacadeHealtcheckTemplateRestClient client;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    private static class PgFacadeLoadBalancerStateInfo {
        private ExternalLoadBalancerHealtcheckTemplateRestClient healtcheckClient;
        private String adapterIdentifier;
        private Instant createdWhen;
    }
}
