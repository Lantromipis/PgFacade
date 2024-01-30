package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.producers.RuntimePostgresConnectionProducer;
import com.lantromipis.configuration.properties.constant.PostgresSettingsConstants;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.PlatformAdapterNotFoundException;
import com.lantromipis.orchestration.exception.RaftException;
import com.lantromipis.orchestration.model.PostgresAdapterInstanceInfo;
import com.lantromipis.orchestration.model.PostgresCombinedInstanceInfo;
import com.lantromipis.orchestration.model.PostgresInstanceCreationRequest;
import com.lantromipis.orchestration.model.SelectedForPromotionStandby;
import com.lantromipis.orchestration.model.raft.PostgresPersistedInstanceInfo;
import com.lantromipis.orchestration.service.api.PostgresConfigurationService;
import com.lantromipis.orchestration.service.api.PostgresHealthcheckService;
import com.lantromipis.orchestration.service.api.PostgresStandbyOrchestrationService;
import com.lantromipis.orchestration.util.OrchestratorUtils;
import com.lantromipis.orchestration.util.PostgresUtils;
import com.lantromipis.orchestration.util.RaftFunctionalityCombinator;
import com.lantromipis.postgresprotocol.utils.LogSequenceNumberUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.postgresql.PGProperty;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

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

    @Inject
    PostgresHealthcheckService postgresHealthcheckService;

    @Inject
    OrchestrationProperties orchestrationProperties;

    private Map<UUID, Integer> standbyByFailedHealthcheckAttempts = new HashMap<>();

    private final static String GET_CURRENT_LSN_QUERY = "SELECT pg_current_wal_lsn()";

    @Override
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

    @Override
    public void checkStandbyCountAndLiveliness() {
        int healthyStandbyCount = 0;

        List<PostgresPersistedInstanceInfo> persistedInstanceInfos = raftFunctionalityCombinator.getPostgresNodeInfos();
        for (PostgresPersistedInstanceInfo persistedInstanceInfo : persistedInstanceInfos) {
            if (persistedInstanceInfo.isPrimary()) {
                continue;
            }

            PostgresAdapterInstanceInfo adapterInstanceInfo;
            try {
                adapterInstanceInfo = platformAdapter.get().getPostgresInstanceInfo(persistedInstanceInfo.getAdapterIdentifier());
            } catch (PlatformAdapterNotFoundException e) {
                log.error("Standby with name {} and exists in Raft but can not be found by adapter. Removing it from Raft!", persistedInstanceInfo.getServerName());
                raftFunctionalityCombinator.deletePostgresNodeInfoInRaft(persistedInstanceInfo.getInstanceId());
                continue;
            }

            boolean standbyHealthy = postgresHealthcheckService.checkPostgresLiveliness(
                    adapterInstanceInfo.getInstanceAddress(),
                    adapterInstanceInfo.getInstancePort(),
                    orchestrationProperties.common().postgresHealthcheckTimeout().toMillis()
            );

            if (!standbyHealthy) {
                log.info("Found unhealthy or inactive standby. Removing it.");
                removeStandby(persistedInstanceInfo);
                continue;
            }

            healthyStandbyCount++;
        }

        int countDiff = orchestrationProperties.common().standby().count() - healthyStandbyCount;

        if (healthyStandbyCount < orchestrationProperties.common().standby().count() && raftFunctionalityCombinator.testIfAbleToCommitToRaftNoException()) {
            log.warn("Found {} healthy standby while it is required to have {}. Need to start {} more.",
                    healthyStandbyCount,
                    orchestrationProperties.common().standby().count(),
                    countDiff
            );

            for (int i = 0; i < countDiff; i++) {
                PostgresCombinedInstanceInfo combinedInstanceInfo = createStartAndWaitForNewStandbyToBeReady();

                if (combinedInstanceInfo == null) {
                    log.error("Failed to create new standby!");
                    return;
                }

                try {
                    raftFunctionalityCombinator.savePostgresNodeInfoInRaft(combinedInstanceInfo.getPersisted());
                    log.info("Standby is up and running!");
                } catch (Exception e) {
                    log.error("Standby was created, but PgFacade failed to safe it's info in Raft! Removing standby...", e);
                    platformAdapter.get().deleteInstance(combinedInstanceInfo.getAdapter().getAdapterInstanceId());
                    postgresUtils.dropPhysicalReplicationSlotOnPrimarySafely(combinedInstanceInfo.getPersisted().getReplicationSlotName());
                }

            }
        } else if (healthyStandbyCount > orchestrationProperties.common().standby().count()) {
            log.warn("Found {} starting or healthy standby while it is required to have {}. Will stop {} instance(s).",
                    healthyStandbyCount,
                    orchestrationProperties.common().standby().count(),
                    Math.abs(countDiff)
            );

            int leftToRemove = Math.abs(countDiff);
            for (PostgresPersistedInstanceInfo persistedInstanceInfo : persistedInstanceInfos) {
                if (leftToRemove <= 0) {
                    break;
                }
                if (persistedInstanceInfo.isPrimary()) {
                    continue;
                }
                removeStandby(persistedInstanceInfo);
                leftToRemove--;
            }
        }
    }

    @Override
    public SelectedForPromotionStandby selectStandbyForPromotion() {
        List<PostgresCombinedInstanceInfo> availableStandbys = orchestratorUtils.getCombinedInfosForStandbyInstances();

        // for single standby, just return it
        if (availableStandbys.size() == 1) {
            PostgresCombinedInstanceInfo singleStandbyInfo = availableStandbys.getFirst();

            boolean singleStandbyHealthy = postgresHealthcheckService.checkPostgresLiveliness(
                    singleStandbyInfo.getAdapter().getInstanceAddress(),
                    singleStandbyInfo.getAdapter().getInstancePort(),
                    orchestrationProperties.common().postgresHealthcheckTimeout().toMillis()
            );

            if (!singleStandbyHealthy) {
                return null;
            }

            try {
                PGProperty.LOGIN_TIMEOUT
                Connection connection = runtimePostgresConnectionProducer.createNewPgFacadeUserConnection(
                        singleStandbyInfo.getAdapter().getInstanceAddress(),
                        singleStandbyInfo.getAdapter().getInstancePort()
                );

                return SelectedForPromotionStandby
                        .builder()
                        .standbyInfo(singleStandbyInfo)
                        .connection(connection)
                        .build();
            } catch (Exception e) {
                log.error("Failed to establish JDBC connection for single standby while trying to select it for promotion!", e);
                return null;
            }
        }

        // for multiple standbys, select one based on LSN
        long latestLsn = LogSequenceNumberUtils.INVALID_LSN;
        PostgresCombinedInstanceInfo retStandby = null;
        Connection retStandbyConnection = null;

        for (PostgresCombinedInstanceInfo standbyInfo : availableStandbys) {
            if (standbyInfo.getAdapter() == null || !standbyInfo.getAdapter().isActive()) {
                continue;
            }

            boolean healthy = postgresHealthcheckService.checkPostgresLiveliness(
                    standbyInfo.getAdapter().getInstanceAddress(),
                    standbyInfo.getAdapter().getInstancePort(),
                    orchestrationProperties.common().postgresHealthcheckTimeout().toMillis()
            );

            if (!healthy) {
                return null;
            }

            try {
                Connection connection = runtimePostgresConnectionProducer.createNewPgFacadeUserConnection(
                        standbyInfo.getAdapter().getInstanceAddress(),
                        standbyInfo.getAdapter().getInstancePort()
                );
            } catch (Exception e) {
                log.error("Failed to establish JDBC connection for standby while trying to check if can be promoted!", e);
            }
        }

        if (retStandby == null || retStandbyConnection == null) {
            return null;
        }


        return SelectedForPromotionStandby
                .builder()
                .standbyInfo(retStandby)
                .connection(retStandbyConnection)
                .build();
    }

    private void removeStandby(PostgresPersistedInstanceInfo standbyInstanceInfo) {
        try {
            raftFunctionalityCombinator.deletePostgresNodeInfoInRaft(standbyInstanceInfo.getInstanceId());
            platformAdapter.get().deleteInstance(standbyInstanceInfo.getAdapterIdentifier());
            postgresUtils.dropPhysicalReplicationSlotOnPrimarySafely(standbyInstanceInfo.getReplicationSlotName());
        } catch (RaftException e) {
            log.error("Failed to delete instance with name {} in raft!", standbyInstanceInfo.getServerName(), e);
        }
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

    private PostgresCombinedInstanceInfo createStartAndWaitForNewStandbyToBeReady() {
        UUID futureInstanceId = UUID.randomUUID();
        String serverName = postgresUtils.createPostgresServerName(futureInstanceId);
        String physicalSlotName = postgresUtils.createPostgresReplicationSlotName(futureInstanceId);

        try (Connection primaryConnection = runtimePostgresConnectionProducer.createNewPgFacadeUserConnectionToCurrentPrimary()) {
            postgresUtils.createPhysicalReplicationSlot(primaryConnection, physicalSlotName);
        } catch (Exception e) {
            log.error("Failed to create new replication slot on primary! possible to create standby!", e);
            return null;
        }

        String adapterIdentifier;
        try {
            adapterIdentifier = platformAdapter.get().createNewPostgresStandbyInstance(
                    PostgresInstanceCreationRequest
                            .builder()
                            .futureInstanceId(UUID.randomUUID())
                            .build()
            );
        } catch (Exception e) {
            log.error("Failed to create new Postgres standby!", e);
            postgresUtils.dropPhysicalReplicationSlotOnPrimarySafely(physicalSlotName);
            return null;
        }

        PostgresAdapterInstanceInfo postgresAdapterInstanceInfo = orchestratorUtils.startPostgresInstanceAndWaitToBeReady(adapterIdentifier);
        if (postgresAdapterInstanceInfo == null) {
            log.error("Newly created standby failed to start!");
            platformAdapter.get().deleteInstance(adapterIdentifier);
            postgresUtils.dropPhysicalReplicationSlotOnPrimarySafely(physicalSlotName);
            return null;
        }

        boolean settingsChanged = configureStandbyForReplication(
                postgresAdapterInstanceInfo,
                serverName,
                physicalSlotName
        );

        if (!settingsChanged) {
            log.error("Failed to set default settings for new Postgres instance!");
            platformAdapter.get().deleteInstance(adapterIdentifier);
            postgresUtils.dropPhysicalReplicationSlotOnPrimarySafely(physicalSlotName);
            return null;
        }

        PostgresPersistedInstanceInfo persistedInstanceInfo = PostgresPersistedInstanceInfo
                .builder()
                .primary(false)
                .instanceId(futureInstanceId)
                .adapterIdentifier(adapterIdentifier)
                .serverName(serverName)
                .replicationSlotName(physicalSlotName)
                .build();

        return PostgresCombinedInstanceInfo
                .builder()
                .adapter(postgresAdapterInstanceInfo)
                .persisted(persistedInstanceInfo)
                .build();
    }
}
