package com.lantromipis.orchestration.util;

import com.lantromipis.configuration.event.StandbyAddedEvent;
import com.lantromipis.configuration.event.StandbyRemovedEvent;
import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.PlatformAdapterNotFoundException;
import com.lantromipis.orchestration.model.PostgresAdapterInstanceInfo;
import com.lantromipis.orchestration.model.PostgresCombinedInstanceInfo;
import com.lantromipis.orchestration.model.raft.PostgresPersistedInstanceInfo;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class containing some methods to reduce code length in Orchestrator. Such methods shouldn't contain a lot of business logic.
 */
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
}
