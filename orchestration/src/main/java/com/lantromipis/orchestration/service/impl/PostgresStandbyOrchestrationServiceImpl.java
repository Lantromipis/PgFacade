package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.producers.RuntimePostgresConnectionProducer;
import com.lantromipis.configuration.properties.constant.PostgresSettingsConstants;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.PlatformAdapterNotFoundException;
import com.lantromipis.orchestration.model.PostgresAdapterInstanceInfo;
import com.lantromipis.orchestration.model.PostgresCombinedInstanceInfo;
import com.lantromipis.orchestration.model.raft.PostgresPersistedInstanceInfo;
import com.lantromipis.orchestration.service.api.PostgresConfigurationService;
import com.lantromipis.orchestration.service.api.PostgresStandbyOrchestrationService;
import com.lantromipis.orchestration.util.OrchestratorUtils;
import com.lantromipis.orchestration.util.PostgresUtils;
import com.lantromipis.orchestration.util.RaftFunctionalityCombinator;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@ApplicationScoped
public class PostgresStandbyOrchestrationServiceImpl implements PostgresStandbyOrchestrationService {

    @Inject
    OrchestratorUtils orchestratorUtils;

    @Inject
    RaftFunctionalityCombinator raftFunctionalityCombinator;

    @Inject
    Instance<PlatformAdapter> platformAdapter;

    @Inject
    RuntimePostgresConnectionProducer runtimePostgresConnectionProducer;

    @Inject
    PostgresConfigurationService postgresConfigurationService;

    @Inject
    PostgresUtils postgresUtils;

    public List<PostgresCombinedInstanceInfo> startStoppedStandbys() {
        List<PostgresCombinedInstanceInfo> ret = new ArrayList<>();
        List<PostgresPersistedInstanceInfo> persistedInstanceInfos = raftFunctionalityCombinator.getPostgresNodeInfos();

        log.info("Starting existing Postgres standbys.");

        for (PostgresPersistedInstanceInfo persistedStandbyInfo : persistedInstanceInfos) {
            if (persistedStandbyInfo.isPrimary()) {
                continue;
            }

            PostgresAdapterInstanceInfo adapterStandbyInfo;
            try {
                adapterStandbyInfo = platformAdapter.get().getPostgresInstanceInfo(persistedStandbyInfo.getAdapterIdentifier());
            } catch (PlatformAdapterNotFoundException e) {
                log.error("Standby with name {} exists in Raft storage but can not be found by adapter. Removing it from Raft!", persistedStandbyInfo.getServerName());
                raftFunctionalityCombinator.deletePostgresNodeInfoInRaft(persistedStandbyInfo.getInstanceId());
                continue;
            } catch (Exception e) {
                log.error("Failed to retrieve info about Postgres standby with name {} using platform adapter. Removing it from Raft!", persistedStandbyInfo.getServerName());
                raftFunctionalityCombinator.deletePostgresNodeInfoInRaft(persistedStandbyInfo.getInstanceId());
                continue;
            }

            if (adapterStandbyInfo.isActive()) {
                log.info("Known standby with name {} is already running!", persistedStandbyInfo.getServerName());
            } else {

                log.info("Found inactive standby with name {}. Will start it now.", persistedStandbyInfo.getServerName());
                PostgresAdapterInstanceInfo standbyAdapterInstanceInfo = orchestratorUtils.startPostgresInstanceAndWaitToBeReady(persistedStandbyInfo.getAdapterIdentifier());
                if (standbyAdapterInstanceInfo == null) {
                    log.error("Failed to start inactive standby with name {}. Will remove it.", persistedStandbyInfo.getServerName());
                    raftFunctionalityCombinator.deletePostgresNodeInfoInRaft(persistedStandbyInfo.getInstanceId());
                    platformAdapter.get().deleteInstance(persistedStandbyInfo.getAdapterIdentifier());
                    continue;
                }
            }

            log.info("Configuring standby with name {} to follow Primary...", persistedStandbyInfo.getServerName());

            boolean configuredSuccessfully = configureStandbyForReplication(
                    adapterStandbyInfo,
                    persistedStandbyInfo.getServerName(),
                    persistedStandbyInfo.getReplicationSlotName()
            );

            if (!configuredSuccessfully) {
                log.error("Failed to configure standby with name {}. Will remove it.", persistedStandbyInfo.getServerName());
                raftFunctionalityCombinator.deletePostgresNodeInfoInRaft(persistedStandbyInfo.getInstanceId());
                platformAdapter.get().deleteInstance(adapterStandbyInfo.getAdapterInstanceId());
            }

            log.info("Standby with name {} is up ad running!", persistedStandbyInfo.getServerName());
        }

        return ret;
    }

    private boolean configureStandbyForReplication(PostgresAdapterInstanceInfo adapterInstanceInfo, String standbyName, String replicationSlotName) {
        boolean restartRequired = false;

        try (Connection connection = runtimePostgresConnectionProducer.createNewPgFacadeUserConnection(
                adapterInstanceInfo.getInstanceAddress(),
                adapterInstanceInfo.getInstancePort()
        )) {
            Map<String, String> expectedSettings = new LinkedHashMap<>();
            expectedSettings.put(
                    PostgresSettingsConstants.CLUSTER_NAME_SETTING_NAME,
                    standbyName
            );
            expectedSettings.put(
                    PostgresSettingsConstants.PRIMARY_SLOT_NAME_SETTING_NAME,
                    replicationSlotName
            );
            expectedSettings.put(
                    PostgresSettingsConstants.PRIMARY_CONN_INFO_SETTING_NAME,
                    postgresUtils.getPrimaryConnInfoSetting()
            );

            Statement statement = connection.createStatement();
            ResultSet pgSettingsResultSet = statement.executeQuery("SELECT name, setting from pgfacade.pg_catalog.pg_settings");

            while (pgSettingsResultSet.next()) {
                String settingName = pgSettingsResultSet.getString(PostgresSettingsConstants.PG_SETTING_COLUMN_SETTING_NAME);
                String settingValue = pgSettingsResultSet.getString(PostgresSettingsConstants.PG_SETTING_COLUMN_SETTING_VALUE);
                String settingContext = pgSettingsResultSet.getString(PostgresSettingsConstants.PG_SETTING_COLUMN_SETTING_CONTEXT);

                String expectedSettingValue = expectedSettings.get(settingName);
                if (expectedSettingValue == null) {
                    continue;
                }

                if (expectedSettingValue.equals(settingValue)) {
                    expectedSettings.remove(settingName);
                    continue;
                }

                if (PostgresSettingsConstants.RESTART_REQUIRED_SETTINGS_CONTEXT_NAMES.contains(settingContext)) {
                    restartRequired = true;
                }
            }

            statement.close();

            if (MapUtils.isEmpty(expectedSettings)) {
                return true;
            }

            boolean settingsChanged = postgresConfigurationService.changePostgresSettingsFastUnsafe(
                    expectedSettings,
                    false,
                    connection
            );

            if (!settingsChanged) {
                log.error("Failed to change settings which are required for replication for started standby!");
                return false;
            }
        } catch (Exception e) {
            log.error("Failed to configure started standby!", e);
            return false;
        }

        if (restartRequired) {
            PostgresAdapterInstanceInfo newAdapterInstanceInfo = orchestratorUtils.restartPostgresInstanceAndWaitToBeReady(adapterInstanceInfo.getAdapterInstanceId());
            if (newAdapterInstanceInfo == null) {
                log.error("Failed to restart standby after configuring it for replication!");
                return false;
            }
        }

        return true;
    }
}
