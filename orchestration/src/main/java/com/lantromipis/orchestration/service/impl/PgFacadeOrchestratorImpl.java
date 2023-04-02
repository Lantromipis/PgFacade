package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.properties.constant.PgFacadeConstants;
import com.lantromipis.configuration.properties.predefined.RaftProperties;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.RaftException;
import com.lantromipis.orchestration.model.PgFacadeInstanceInfo;
import com.lantromipis.orchestration.model.PgFacadeRaftNodeInfo;
import com.lantromipis.orchestration.restclient.HealtcheckTemplateRestClient;
import com.lantromipis.orchestration.restclient.model.HealtcheckResponseDto;
import com.lantromipis.orchestration.service.api.PgFacadeOrchestrator;
import com.lantromipis.orchestration.service.api.PgFacadeRaftService;
import com.lantromipis.pgfacadeprotocol.model.api.RaftPeerInfo;
import io.quarkus.scheduler.Scheduled;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.rest.client.RestClientBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
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

    private ConcurrentHashMap<String, PgFacadeInstanceInfo> instances = new ConcurrentHashMap<>();

    @Override
    public void startOrchestration() {
        log.info("Starting PgFacade orchestration!");
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
                if (!instances.containsKey(raftPeer.getId())) {
                    instances.put(
                            raftPeer.getId(),
                            PgFacadeInstanceInfo
                                    .builder()
                                    .raftIdAndAdapterId(raftPeer.getId())
                                    .client(createPgFacadeHealtcheckRestClient(raftPeer.getIpAddress()))
                                    .unsuccessfulHealtcheckCount(new AtomicInteger(0))
                                    .build()
                    );
                }
            }

            for (var instance : instances.values()) {
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
                    instances.put(
                            raftNodeInfo.getPlatformAdapterIdentifier(),
                            PgFacadeInstanceInfo
                                    .builder()
                                    .raftIdAndAdapterId(raftNodeInfo.getPlatformAdapterIdentifier())
                                    .client(createPgFacadeHealtcheckRestClient(raftNodeInfo.getAddress()))
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
        Optional.ofNullable(instances.get(id))
                .map(PgFacadeInstanceInfo::getClient)
                .ifPresent(
                        client -> {
                            try {
                                client.close();
                            } catch (Exception ignored) {
                            }
                        }
                );
        pgFacadeRaftService.removeNode(id);
        instances.remove(id);
        platformAdapter.get().deleteInstance(id);
    }

    private HealtcheckTemplateRestClient createPgFacadeHealtcheckRestClient(String address) {
        URI uri = URI.create("http://" + address + ":" + pgFacadeRuntimeProperties.getHttpPort());

        return RestClientBuilder.newBuilder()
                .baseUri(uri)
                .connectTimeout(1000, TimeUnit.MILLISECONDS)
                .readTimeout(1000, TimeUnit.MILLISECONDS)
                .build(HealtcheckTemplateRestClient.class);
    }

    private void awaitNewRaftNodeReadiness(String address) throws RaftException {
        try (HealtcheckTemplateRestClient healtcheckRestClient = createPgFacadeHealtcheckRestClient(address)) {
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
}
