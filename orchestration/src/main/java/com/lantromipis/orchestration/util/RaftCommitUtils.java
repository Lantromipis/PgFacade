package com.lantromipis.orchestration.util;

import com.lantromipis.configuration.event.MaxConnectionsChangedEvent;
import com.lantromipis.configuration.event.SwitchoverCompletedEvent;
import com.lantromipis.configuration.event.SwitchoverStartedEvent;
import com.lantromipis.configuration.model.PostgresPersistedInstanceInfo;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.constant.PostgresConstants;
import com.lantromipis.orchestration.model.PostgresAdapterInstanceInfo;
import com.lantromipis.orchestration.model.PostgresCombinedInstanceInfo;
import com.lantromipis.orchestration.service.api.raft.RaftStorage;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.Map;
import java.util.UUID;

@Slf4j
@ApplicationScoped
public class RaftCommitUtils {

    @Inject
    Instance<PlatformAdapter> platformAdapter;

    @Inject
    OrchestratorUtils orchestratorUtils;

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    Event<MaxConnectionsChangedEvent> maxConnectionsChangedEvent;

    @Inject
    Event<SwitchoverStartedEvent> switchoverStartedEvent;

    @Inject
    Event<SwitchoverCompletedEvent> switchoverCompletedEvent;

    @Inject
    RaftStorage raftStorage;

    public void processInstallSnapshot() {
        raftStorage.getPostgresNodeInfos().forEach(
                info -> {
                    try {
                        PostgresAdapterInstanceInfo adapterInstanceInfo = platformAdapter.get().getPostgresInstanceInfo(
                                info.getAdapterIdentifier()
                        );

                        orchestratorUtils.addInstanceToRuntimePropertiesAndNotifyAllIfStandby(
                                PostgresCombinedInstanceInfo
                                        .builder()
                                        .persisted(info)
                                        .adapter(adapterInstanceInfo)
                                        .build()
                        );
                    } catch (Exception e) {
                        log.error("Failed to add instance to runtime properties, when installing new snapshot from Raft. Maybe instance is removed.");
                    }
                }
        );
    }

    public void processCommittedSwitchoverStartedEvent(SwitchoverStartedEvent event) {
        switchoverStartedEvent.fire(event);
    }

    public void processCommittedSwitchoverCompletedEvent(SwitchoverCompletedEvent event) {
        switchoverCompletedEvent.fire(event);
    }

    public void processCommittedSavePostgresNodeInfoCommand(PostgresPersistedInstanceInfo committedInfo) {
        try {
            PostgresAdapterInstanceInfo adapterInstanceInfo = platformAdapter.get().getPostgresInstanceInfo(
                    committedInfo.getAdapterIdentifier()
            );

            orchestratorUtils.addInstanceToRuntimePropertiesAndNotifyAllIfStandby(
                    PostgresCombinedInstanceInfo
                            .builder()
                            .persisted(committedInfo)
                            .adapter(adapterInstanceInfo)
                            .build()
            );

        } catch (Exception e) {
            log.error("Failed to add instance to runtime properties, after its info was committed in Raft. Maybe instance is removed.");
        }
    }

    public void processCommittedDeletePostgresNodeInfoCommand(UUID deletedInstanceId) {
        orchestratorUtils.removeInstanceFromRuntimePropertiesAndNotifyAllIfStandby(deletedInstanceId);
    }

    public void processCommittedClearPostgresNodeInfoCommand() {
        clusterRuntimeProperties.getAllPostgresInstancesInfos()
                .forEach((uuid, instanceInfo) -> orchestratorUtils.removeInstanceFromRuntimePropertiesAndNotifyAllIfStandby(uuid));
    }

    public void processCommittedPostgresSettingsInfoCommand(Map<String, String> committedSettings) {
        String maxConnections = committedSettings.get(PostgresConstants.MAX_CONNECTIONS_SETTING_NAME);
        if (maxConnections != null) {
            int maxConnectionsInt = Integer.parseInt(maxConnections);
            clusterRuntimeProperties.setMaxPostgresConnections(maxConnectionsInt);
            maxConnectionsChangedEvent.fire(new MaxConnectionsChangedEvent());
        }
    }
}
