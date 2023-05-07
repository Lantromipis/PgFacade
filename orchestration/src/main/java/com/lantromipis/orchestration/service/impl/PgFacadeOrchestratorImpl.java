package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.event.RaftLogSyncedOnStartupEvent;
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
import com.lantromipis.orchestration.restclient.PgFacadeShutdownTemplateRestClient;
import com.lantromipis.orchestration.restclient.model.ForceShutdownRequestDto;
import com.lantromipis.orchestration.restclient.model.HealtcheckResponseDto;
import com.lantromipis.orchestration.restclient.model.SoftShutdownRequestDto;
import com.lantromipis.orchestration.service.api.PgFacadeOrchestrator;
import com.lantromipis.orchestration.service.api.PgFacadeRaftService;
import com.lantromipis.orchestration.service.api.PostgresOrchestrator;
import com.lantromipis.orchestration.util.RaftFunctionalityCombinator;
import com.lantromipis.pgfacadeprotocol.model.api.RaftPeerInfo;
import com.lantromipis.proxy.service.api.PgProxyService;
import io.quarkus.runtime.Quarkus;
import io.quarkus.scheduler.Scheduled;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.eclipse.microprofile.rest.client.RestClientBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.io.Closeable;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

    @Inject
    ManagedExecutor managedExecutor;

    @Inject
    PgProxyService proxyService;

    @Inject
    PostgresOrchestrator postgresOrchestrator;

    private ConcurrentHashMap<String, PgFacadeInstanceStateInfo> pgFacadeInstances = new ConcurrentHashMap<>();

    private boolean orchestrationActive = false;
    private boolean raftSynced = false;
    private String loadBalancerAdapterId;

    @Override
    public void startOrchestration() {
        if (!pgFacadeRuntimeProperties.isPgFacadeOrchestrationForceDisabled()) {
            orchestrationActive = true;
            log.info("Starting PgFacade orchestration!");
        } else {
            log.info("Can not start PgFacade orchestration because it is disabled by HTTP request!");
        }
    }

    @Override
    public void stopOrchestration() {
        if (orchestrationActive) {
            orchestrationActive = false;
            log.info("Stopped PgFacade orchestration!");
        }
    }

    @Override
    public boolean shutdownCluster(boolean force, boolean shutdownPostgres, boolean shutdownLoadBalancer, long maxProxyAwaitSeconds) {
        log.info("Cluster shutdown requested.");

        AtomicBoolean success = new AtomicBoolean(true);
        AtomicBoolean leader = new AtomicBoolean(false);

        try {
            // Prevent orchestrator from creating other nodes
            pgFacadeRuntimeProperties.setPgFacadeOrchestrationForceDisabled(true);
            stopOrchestration();

            // Leader shutdowns all followers. Followers just shutdown themselves.
            if (PgFacadeRaftRole.LEADER.equals(pgFacadeRuntimeProperties.getRaftRole())) {
                leader.set(true);
                pgFacadeInstances.values()
                        .forEach(
                                instance -> {
                                    try (PgFacadeShutdownTemplateRestClient shutdownRestClient = createRestClient(
                                            PgFacadeShutdownTemplateRestClient.class,
                                            instance.getAddress(),
                                            pgFacadeRuntimeProperties.getHttpPort(),
                                            30000
                                    )) {
                                        if (force) {
                                            shutdownRestClient.shutdownForce(
                                                    ForceShutdownRequestDto
                                                            .builder()
                                                            .build()
                                            );
                                        } else {
                                            shutdownRestClient.shutdownSoft(
                                                    SoftShutdownRequestDto
                                                            .builder()
                                                            .maxClientsAwaitPeriodSeconds(maxProxyAwaitSeconds)
                                                            .build()
                                            );
                                        }
                                    } catch (Exception e) {
                                        success.set(false);
                                        log.error("Failed to shutdown Raft Server on node with address {}", instance.getAddress());
                                    }
                                }
                        );
            }

            pgFacadeRaftService.shutdown(true);

            if (force) {
                proxyService.shutdown(false, null);
                postgresOrchestrator.stopOrchestrator(shutdownPostgres && leader.get());
                if (shutdownLoadBalancer && leader.get() && loadBalancerAdapterId != null) {
                    platformAdapter.get().deleteInstance(loadBalancerAdapterId);
                }
                log.info("All Postgres instances stopped.");
                managedExecutor.runAsync(() -> {
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException e) {
                                // ignored
                            } finally {
                                Quarkus.asyncExit(0);
                            }
                        }
                );
            } else {
                managedExecutor.runAsync(() -> {
                            proxyService.shutdown(true, Duration.ofSeconds(maxProxyAwaitSeconds));
                            log.info("All clients disconnected. Shutting down PgFacade...");
                            postgresOrchestrator.stopOrchestrator(shutdownPostgres && leader.get());
                            if (shutdownLoadBalancer && leader.get() && loadBalancerAdapterId != null) {
                                platformAdapter.get().deleteInstance(loadBalancerAdapterId);
                            }
                            log.info("All Postgres instances stopped.");
                            Quarkus.asyncExit(0);
                        }
                );
            }
        } catch (Exception e) {
            success.set(false);
            log.error("Error while shutting down Raft!");
        }

        return success.get();
    }

    public void syncedWithRaftLog(@Observes RaftLogSyncedOnStartupEvent raftLogSyncedOnStartupEvent) {
        raftSynced = true;
    }

    @Scheduled(every = "${pg-facade.orchestration.common.external-load-balancer.healthcheck-interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void checkExternalLoadBalancerHealth() {
        if (orchestrationActive && PgFacadeRaftRole.LEADER.equals(pgFacadeRuntimeProperties.getRaftRole()) && raftSynced && orchestrationProperties.common().externalLoadBalancer().deploy()) {
            ExternalLoadBalancerHealtcheckTemplateRestClient restClient = null;
            ExternalLoadBalancerRaftInfo raftInfo = null;

            try {
                raftInfo = raftFunctionalityCombinator.getPgFacadeLoadBalancerInfo();

                if (raftInfo != null) {
                    loadBalancerAdapterId = raftInfo.getAdapterIdentifier();
                    if (raftInfo.getCreatedWhen().isAfter(Instant.now().minus(orchestrationProperties.common().externalLoadBalancer().healthcheckAwait()))) {
                        return;
                    }
                    restClient = createRestClient(
                            ExternalLoadBalancerHealtcheckTemplateRestClient.class,
                            raftInfo.getAddress(),
                            raftInfo.getPort()
                    );

                    if (HealtcheckResponseDto.HealtcheckStatus.UP.equals(restClient.checkLiveliness().getStatus())) {
                        return;
                    } else {
                        log.error("External load balancer unhealthy! Will force redeploy it.");
                        platformAdapter.get().deleteInstance(raftInfo.getAdapterIdentifier());
                    }
                } else {
                    log.info("No information about deployed load balancer found.");
                }
            } catch (Exception e) {
                if (raftInfo != null && raftInfo.getAdapterIdentifier() != null) {
                    platformAdapter.get().deleteInstance(raftInfo.getAdapterIdentifier());
                }
                log.error("Failed to check load balancer health. Will redeploy it...", e);
            } finally {
                if (restClient != null) {
                    closeClient(restClient);
                }
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
                        .createdWhen(Instant.now())
                        .build();
                raftFunctionalityCombinator.savePgFacadeLoadBalancerInfo(newRaftInfo);
                loadBalancerAdapterId = loadBalancerAdapterInfo.getAdapterIdentifier();
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
        if (orchestrationActive && PgFacadeRaftRole.LEADER.equals(pgFacadeRuntimeProperties.getRaftRole())) {
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
                                    .address(raftPeer.getIpAddress())
                                    .client(createRestClient(PgFacadeHealtcheckTemplateRestClient.class, raftPeer.getIpAddress(), pgFacadeRuntimeProperties.getHttpPort()))
                                    .unsuccessfulHealtcheckCount(new AtomicInteger(0))
                                    .build()
                    );
                }
            }

            for (var instance : pgFacadeInstances.values()) {
                try {
                    RaftPeerInfo peerInfo = raftPeerInfos.get(instance.getRaftIdAndAdapterId());
                    if (peerInfo != null
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
                                    .address(raftNodeInfo.getAddress())
                                    .client(createRestClient(PgFacadeHealtcheckTemplateRestClient.class, raftNodeInfo.getAddress(), pgFacadeRuntimeProperties.getHttpPort()))
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

    private <T> T createRestClient(Class<T> clazz, String address, int port) {
        URI uri = URI.create("http://" + address + ":" + port);

        return RestClientBuilder.newBuilder()
                .baseUri(uri)
                .connectTimeout(1000, TimeUnit.MILLISECONDS)
                .readTimeout(1000, TimeUnit.MILLISECONDS)
                .build(clazz);
    }

    private <T> T createRestClient(Class<T> clazz, String address, int port, long timeout) {
        URI uri = URI.create("http://" + address + ":" + port);

        return RestClientBuilder.newBuilder()
                .baseUri(uri)
                .connectTimeout(timeout, TimeUnit.MILLISECONDS)
                .readTimeout(timeout, TimeUnit.MILLISECONDS)
                .build(clazz);
    }

    private void closeClient(Closeable client) {
        try {
            client.close();
        } catch (Exception ignored) {
        }
    }

    private void awaitNewRaftNodeReadiness(String address) throws RaftException {
        try (PgFacadeHealtcheckTemplateRestClient healtcheckRestClient = createRestClient(PgFacadeHealtcheckTemplateRestClient.class, address, pgFacadeRuntimeProperties.getHttpPort())) {
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
}
