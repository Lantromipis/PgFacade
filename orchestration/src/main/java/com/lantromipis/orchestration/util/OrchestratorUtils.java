package com.lantromipis.orchestration.util;

import com.lantromipis.configuration.event.StandbyAddedEvent;
import com.lantromipis.configuration.event.StandbyRemovedEvent;
import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.PlatformAdapterNotFoundException;
import com.lantromipis.orchestration.model.PostgresAdapterInstanceInfo;
import com.lantromipis.orchestration.model.PostgresCombinedInstanceInfo;
import com.lantromipis.orchestration.model.raft.PostgresPersistedInstanceInfo;
import com.lantromipis.orchestration.service.api.PostgresHealthcheckService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class containing some methods to reduce code length in Orchestrator. Such methods shouldn't contain a lot of business logic.
 */
@Slf4j
@ApplicationScoped
public class OrchestratorUtils {
    @Inject
    RaftFunctionalityCombinator raftFunctionalityCombinator;

    @Inject
    Instance<PlatformAdapter> platformAdapter;

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    Event<StandbyAddedEvent> standbyAddedEvent;

    @Inject
    Event<StandbyRemovedEvent> standbyRemovedEvent;

    @Inject
    PostgresHealthcheckService postgresHealthcheckService;

    @Inject
    OrchestrationProperties orchestrationProperties;

    public void removeInstanceFromRuntimePropertiesAndNotifyAllIfStandby(UUID instanceId) {
        RuntimePostgresInstanceInfo instanceInfo = clusterRuntimeProperties.getAllPostgresInstancesInfos().remove(instanceId);
        if (instanceInfo != null && !instanceInfo.isPrimary()) {
            standbyRemovedEvent.fire(new StandbyRemovedEvent(instanceId));
        }
    }

    public void addInstanceToRuntimePropertiesAndNotifyAllIfStandby(PostgresCombinedInstanceInfo instanceInfo) {
        clusterRuntimeProperties.getAllPostgresInstancesInfos().put(
                instanceInfo.getPersisted().getInstanceId(),
                RuntimePostgresInstanceInfo
                        .builder()
                        .primary(instanceInfo.getPersisted().isPrimary())
                        .address(instanceInfo.getAdapter().getInstanceAddress())
                        .port(instanceInfo.getAdapter().getInstancePort())
                        .instanceId(instanceInfo.getPersisted().getInstanceId())
                        .build()
        );

        if (!instanceInfo.getPersisted().isPrimary()) {
            standbyAddedEvent.fire(new StandbyAddedEvent(instanceInfo.getPersisted().getInstanceId()));
        }
    }

    public PostgresCombinedInstanceInfo getCombinedInstanceInfo(UUID instanceId) {
        PostgresPersistedInstanceInfo persistedInstanceInf = raftFunctionalityCombinator.getPostgresNodeInfo(instanceId);

        if (persistedInstanceInf == null) {
            return null;
        }

        return PostgresCombinedInstanceInfo
                .builder()
                .adapter(platformAdapter.get().getPostgresInstanceInfo(persistedInstanceInf.getAdapterIdentifier()))
                .persisted(persistedInstanceInf)
                .build();
    }

    public Stream<PostgresCombinedInstanceInfo> getCombinedInfosForAvailableInstancesAsStream() {
        return raftFunctionalityCombinator
                .getPostgresNodeInfos()
                .stream()
                .map(info -> {
                            try {
                                PostgresAdapterInstanceInfo postgresAdapterInstanceInfo = platformAdapter.get().getPostgresInstanceInfo(info.getAdapterIdentifier());
                                return PostgresCombinedInstanceInfo
                                        .builder()
                                        .adapter(postgresAdapterInstanceInfo)
                                        .persisted(info)
                                        .build();
                            } catch (PlatformAdapterNotFoundException notFoundException) {
                                return null;
                            }
                        }
                )
                .filter(Objects::nonNull);
    }

    public Stream<PostgresCombinedInstanceInfo> getCombinedInfosForStandbyInstancesAsStream() {
        return raftFunctionalityCombinator
                .getPostgresNodeInfos()
                .stream()
                .filter(info -> !info.isPrimary())
                .map(info -> {
                            try {
                                PostgresAdapterInstanceInfo postgresAdapterInstanceInfo = platformAdapter.get().getPostgresInstanceInfo(info.getAdapterIdentifier());
                                return PostgresCombinedInstanceInfo
                                        .builder()
                                        .adapter(postgresAdapterInstanceInfo)
                                        .persisted(info)
                                        .build();
                            } catch (PlatformAdapterNotFoundException notFoundException) {
                                return null;
                            }
                        }
                )
                .filter(Objects::nonNull);
    }

    public List<PostgresCombinedInstanceInfo> getCombinedInfosForAvailableInstances() {
        return getCombinedInfosForAvailableInstancesAsStream().collect(Collectors.toList());
    }

    public List<PostgresCombinedInstanceInfo> getCombinedInfosForStandbyInstances() {
        return getCombinedInfosForStandbyInstancesAsStream().collect(Collectors.toList());
    }

    public PostgresAdapterInstanceInfo waitUntilPostgresInstanceHealthy(String adapterInstanceId) {
        OrchestrationProperties.CommonProperties.PostgresStartupCheckProperties startupCheckProperties = orchestrationProperties.common().postgresStartupCheck();

        long endTime = System.currentTimeMillis() + (startupCheckProperties.interval() * startupCheckProperties.retries()) + startupCheckProperties.startPeriod();
        PostgresAdapterInstanceInfo instanceInfo = platformAdapter.get().getPostgresInstanceInfo(adapterInstanceId);

        try {
            boolean succeeded = false;
            while (endTime > System.currentTimeMillis()) {
                boolean healthcheckSucceeded = postgresHealthcheckService.checkPostgresLiveliness(
                        instanceInfo.getInstanceAddress(),
                        instanceInfo.getInstancePort(),
                        orchestrationProperties.common().postgresHealthcheckTimeout().toMillis()
                );

                if (healthcheckSucceeded) {
                    succeeded = true;
                    break;
                }

                Thread.sleep(startupCheckProperties.interval());
                instanceInfo = platformAdapter.get().getPostgresInstanceInfo(adapterInstanceId);
            }

            if (!succeeded) {
                log.error("Failed to achieve healthy Postgres instance. Timeout reached.");
                return null;
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            log.error("Failed to achieve healthy instance.", interruptedException);
            return null;
        } catch (Exception e) {
            log.error("Failed to achieve healthy instance.", e);
            return null;
        }

        return instanceInfo;
    }

    public PostgresAdapterInstanceInfo startPostgresInstanceAndWaitToBeReady(String adapterIdentifier) {
        try {
            boolean started = platformAdapter.get().startPostgresInstance(adapterIdentifier);

            if (!started) {
                log.error("Failed to start Postgres instance!");
                return null;
            }

            PostgresAdapterInstanceInfo adapterInstanceInfo = waitUntilPostgresInstanceHealthy(adapterIdentifier);
            if (adapterInstanceInfo == null) {
                log.error("Started Postgres instance, but failed while waiting for it to become healthy!");
            }

            return adapterInstanceInfo;
        } catch (Exception e) {
            log.error("Error while starting Postgres instance!", e);
            return null;
        }
    }

    public PostgresAdapterInstanceInfo restartPostgresInstanceAndWaitToBeReady(String adapterIdentifier) {
        try {
            platformAdapter.get().restartPostgresInstance(adapterIdentifier);

            PostgresAdapterInstanceInfo adapterInstanceInfo = waitUntilPostgresInstanceHealthy(adapterIdentifier);
            if (adapterInstanceInfo == null) {
                log.error("Postgres instance failed to become healthy after restart!");
            }

            return adapterInstanceInfo;
        } catch (Exception e) {
            log.error("Failed to restart Postgres instance!", e);
            return null;
        }
    }
}
