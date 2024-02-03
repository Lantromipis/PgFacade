package com.lantromipis.orchestration.util;

import com.lantromipis.configuration.event.SwitchoverCompletedEvent;
import com.lantromipis.configuration.event.SwitchoverStartedEvent;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.configuration.properties.runtime.PostgresSettingsRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.model.PostgresAdapterInstanceInfo;
import com.lantromipis.orchestration.model.PostgresCombinedInstanceInfo;
import com.lantromipis.orchestration.model.raft.*;
import com.lantromipis.orchestration.service.api.raft.RaftStorage;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

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
    Event<SwitchoverStartedEvent> switchoverStartedEvent;

    @Inject
    Event<SwitchoverCompletedEvent> switchoverCompletedEvent;

    @Inject
    PostgresSettingsRuntimeProperties postgresSettingsRuntimeProperties;

    @Inject
    RaftStorage raftStorage;

    public void processPgFacadeLoadBalancerInfoSave(ExternalLoadBalancerRaftInfo externalLoadBalancerRaftInfo) {
        raftStorage.savePgFacadeLoadBalancerInfo(externalLoadBalancerRaftInfo);
    }

    public void processArchiveInfoSave(PostgresPersistedArchiverInfo archiveInfo) {
        raftStorage.saveArchiveInfo(archiveInfo);
    }

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

    public void processCommittedSwitchoverStartedNotification(PostgresSwitchoverStartedNotification switchoverStartedNotification) {
        SwitchoverStartedEvent event = new SwitchoverStartedEvent(switchoverStartedNotification.getNotificationId());
        switchoverStartedEvent.fire(event);
    }

    public void processCommittedSwitchoverCompletedNotification(PostgresSwitchoverCompletedNotification switchoverCompletedNotification) {
        SwitchoverCompletedEvent event = new SwitchoverCompletedEvent(
                switchoverCompletedNotification.getNotificationId(),
                switchoverCompletedNotification.isSuccess()
        );

        if (switchoverCompletedNotification.isSuccess()) {
            if (switchoverCompletedNotification.getNewPrimaryCombinedInfo() != null) {
                // delete old info
                orchestratorUtils.removeInstanceFromRuntimePropertiesAndNotifyAllIfStandby(switchoverCompletedNotification.getNewPrimaryCombinedInfo().getPersisted().getInstanceId());
                // save new info
                orchestratorUtils.addInstanceToRuntimePropertiesAndNotifyAllIfStandby(switchoverCompletedNotification.getNewPrimaryCombinedInfo());
                raftStorage.savePostgresNodeInfo(switchoverCompletedNotification.getNewPrimaryCombinedInfo().getPersisted());
            }
            // remove deleted instances
            if (CollectionUtils.isNotEmpty(switchoverCompletedNotification.getInstanceToRemoveIds())) {
                for (UUID instanceToRemoveId : switchoverCompletedNotification.getInstanceToRemoveIds()) {
                    orchestratorUtils.removeInstanceFromRuntimePropertiesAndNotifyAllIfStandby(instanceToRemoveId);
                }
                raftStorage.deletePostgresNodeInfo(switchoverCompletedNotification.getInstanceToRemoveIds());
            }
        }

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

    public void processCommittedUpdatePostgresNodeInfoCommand(PostgresPersistedInstanceInfo updatedCommittedInfo) {
        try {
            // delete old instance info
            orchestratorUtils.removeInstanceFromRuntimePropertiesAndNotifyAllIfStandby(updatedCommittedInfo.getInstanceId());

            // save new info
            PostgresAdapterInstanceInfo adapterInstanceInfo = platformAdapter.get().getPostgresInstanceInfo(
                    updatedCommittedInfo.getAdapterIdentifier()
            );

            orchestratorUtils.addInstanceToRuntimePropertiesAndNotifyAllIfStandby(
                    PostgresCombinedInstanceInfo
                            .builder()
                            .persisted(updatedCommittedInfo)
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

    public void processCommittedPostgresSettingsInfoCommand() throws Exception {
        postgresSettingsRuntimeProperties.reload();
    }
}
