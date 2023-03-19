package com.lantromipis.orchestration.util;

import com.lantromipis.configuration.event.StandbyAddedEvent;
import com.lantromipis.configuration.event.StandbyRemovedEvent;
import com.lantromipis.configuration.model.PostgresPersistedInstanceInfo;
import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.configuration.properties.stored.api.PostgresPersistedProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.model.PostgresCombinedInstanceInfo;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class containing some methods to reduce code length in Orchestrator. Such methods shouldn't contain a lot of business logic.
 */
@ApplicationScoped
public class OrchestratorUtils {
    @Inject
    PostgresPersistedProperties postgresPersistedProperties;

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
        PostgresPersistedInstanceInfo persistedInstanceInf = postgresPersistedProperties.getPostgresNodeInfo(instanceId);

        if (persistedInstanceInf == null) {
            return null;
        }

        return PostgresCombinedInstanceInfo
                .builder()
                .adapter(platformAdapter.get().getInstanceInfo(persistedInstanceInf.getAdapterIdentifier()))
                .persisted(persistedInstanceInf)
                .build();
    }

    public Stream<PostgresCombinedInstanceInfo> getCombinedInfosForAvailableInstancesAsStream() {
        return postgresPersistedProperties
                .getPostgresNodeInfos()
                .stream()
                .map(info ->
                        PostgresCombinedInstanceInfo
                                .builder()
                                .adapter(platformAdapter.get().getInstanceInfo(info.getAdapterIdentifier()))
                                .persisted(info)
                                .build()
                );
    }

    public Stream<PostgresCombinedInstanceInfo> getCombinedInfosForStandbyInstancesAsStream() {
        return postgresPersistedProperties
                .getPostgresNodeInfos()
                .stream()
                .filter(info -> !info.isPrimary())
                .map(info ->
                        PostgresCombinedInstanceInfo
                                .builder()
                                .adapter(platformAdapter.get().getInstanceInfo(info.getAdapterIdentifier()))
                                .persisted(info)
                                .build()
                );
    }

    public List<PostgresCombinedInstanceInfo> getCombinedInfosForAvailableInstances() {
        return getCombinedInfosForAvailableInstancesAsStream().collect(Collectors.toList());
    }

    public List<PostgresCombinedInstanceInfo> getCombinedInfosForStandbyInstances() {
        return getCombinedInfosForStandbyInstancesAsStream().collect(Collectors.toList());
    }
}
