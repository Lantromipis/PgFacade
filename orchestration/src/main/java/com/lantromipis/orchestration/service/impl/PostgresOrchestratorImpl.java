package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.event.*;
import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.model.PostgresPersistedInstanceInfo;
import com.lantromipis.configuration.properties.predefined.ArchivingProperties;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.configuration.properties.stored.api.PostgresPersistedProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.*;
import com.lantromipis.orchestration.model.*;
import com.lantromipis.orchestration.service.api.PostgresArchiver;
import com.lantromipis.orchestration.service.api.PostgresConfigurator;
import com.lantromipis.orchestration.service.api.PostgresOrchestrator;
import com.lantromipis.orchestration.util.OrchestratorUtils;
import com.lantromipis.orchestration.util.PostgresUtils;
import io.quarkus.scheduler.Scheduled;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@ApplicationScoped
public class PostgresOrchestratorImpl implements PostgresOrchestrator {
    @Inject
    Instance<PlatformAdapter> platformAdapter;

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    OrchestrationProperties orchestrationProperties;

    @Inject
    Event<PrimaryReadyEvent> primaryReadyEvent;

    @Inject
    Event<SwitchoverCompletedEvent> switchoverCompletedEvent;

    @Inject
    Event<SwitchoverStartedEvent> switchoverStartedEvent;

    @Inject
    Event<StandbyRemovedEvent> standbyRemovedEvent;

    @Inject
    PostgresConfigurator postgresConfigurator;

    @Inject
    ManagedExecutor managedExecutor;

    @Inject
    PostgresUtils postgresUtils;

    @Inject
    ArchivingProperties archivingProperties;

    @Inject
    PostgresPersistedProperties postgresPersistedProperties;

    @Inject
    PostgresArchiver postgresArchiver;

    @Inject
    OrchestratorUtils orchestratorUtils;

    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    private final AtomicBoolean orchestratorReady = new AtomicBoolean(false);
    private final AtomicBoolean livelinessCheckInProgress = new AtomicBoolean(false);
    private final AtomicBoolean standbyCountCheckInProgress = new AtomicBoolean(false);
    private final AtomicBoolean switchoverInProgress = new AtomicBoolean(false);
    private final AtomicBoolean primaryUnhealthy = new AtomicBoolean(false);
    private int healthcheckFailedCount = 0;
    private final Set<UUID> restartingStandbyInstanceIds = ConcurrentHashMap.newKeySet();

    @Override
    public void initialize() throws InitializationException {
        if (PgFacadeRaftRole.FOLLOWER.equals(pgFacadeRuntimeProperties.getRaftRole())) {
            log.info("Not starting Postgres orchestrator because this PgFacade instance is not current raft leader.");
            postgresConfigurator.initialize();
            return;
        }

        boolean primaryStarted = false;

        PostgresPersistedInstanceInfo primaryPersistedInstanceInfo = postgresPersistedProperties.getPostgresNodeInfos()
                .stream()
                .filter(PostgresPersistedInstanceInfo::isPrimary)
                .findFirst()
                .orElse(null);

        PostgresCombinedInstanceInfo primaryInstanceInfo = null;

        if (primaryPersistedInstanceInfo != null) {
            PostgresAdapterInstanceInfo adapterInstanceInfo = platformAdapter.get().getInstanceInfo(primaryPersistedInstanceInfo.getAdapterIdentifier());

            if (adapterInstanceInfo.isActive()) {
                primaryInstanceInfo = PostgresCombinedInstanceInfo
                        .builder()
                        .adapter(adapterInstanceInfo)
                        .persisted(primaryPersistedInstanceInfo)
                        .build();
                primaryStarted = true;
                log.info("Found active Postgres primary. No actions needed.");

            } else {
                log.info("Found non-active Postgres primary instance. Will start it now.");

                try {
                    platformAdapter.get().startPostgresInstance(adapterInstanceInfo.getAdapterInstanceId());
                    adapterInstanceInfo = waitUntilPostgresInstanceHealthy(adapterInstanceInfo.getAdapterInstanceId());
                    primaryInstanceInfo = PostgresCombinedInstanceInfo
                            .builder()
                            .adapter(adapterInstanceInfo)
                            .persisted(primaryPersistedInstanceInfo)
                            .build();
                    primaryStarted = true;
                    log.info("Successfully started non-active Postgres primary!");
                } catch (Exception e) {
                    log.error("Failed to start non-active Postgres primary!", e);
                }
            }
        }

        if (primaryPersistedInstanceInfo == null || !primaryStarted) {
            if (archivingProperties.enabled() && orchestrationProperties.postgresClusterRestore().autoRestoreIfNoInstancesOnStartup()) {
                primaryInstanceInfo = restoreClusterFromBackup(true);
            } else {
                log.error("No known Postgres primary found but restore from backup is not allowed!");
            }

            if (primaryInstanceInfo == null && orchestrationProperties.postgresClusterRestore().allowCreatingNewEmptyPrimaryIfRestoreOnStartupFailed()) {
                log.info("Will create and start new and empty Primary because configuration allows it.");
                primaryInstanceInfo = createStartAndWaitForNewInstanceToBeReady(true);
                orchestratorUtils.addInstanceToRuntimePropertiesAndNotifyAllIfStandby(primaryInstanceInfo);
                postgresConfigurator.configureNewlyCreatedPrimary(primaryInstanceInfo);

            } else {
                throw new InitializationException("Orchestrator failed to start because there is no active Postgres primary and all attempts to create new one failed. Restore primary manually and restart PgFacade in 'RECOVERY' mode!");
            }
        }

        orchestratorUtils.addInstanceToRuntimePropertiesAndNotifyAllIfStandby(primaryInstanceInfo);

        log.info("Primary is up and running!");

        primaryReadyEvent.fire(new PrimaryReadyEvent());

        // standby section
        List<PostgresCombinedInstanceInfo> standbyInfos = orchestratorUtils.getCombinedInfosForStandbyInstances();

        log.info("Checking standby count.");

        if (CollectionUtils.isNotEmpty(standbyInfos) && standbyInfos.stream().noneMatch(info -> info.getAdapter().isActive())) {
            log.info("All standby inactive. Will start required amount.");
            // All standby inactive which means that PgFacade is starting up after full shutdown. Start required count.
            for (int i = 0; i < Math.min(standbyInfos.size(), orchestrationProperties.common().standby().count()); i++) {
                PostgresCombinedInstanceInfo standbyInfo = standbyInfos.get(i);
                try {
                    boolean standbyStarted = platformAdapter.get().startPostgresInstance(standbyInfo.getPersisted().getAdapterIdentifier());
                    if (standbyStarted) {
                        waitUntilPostgresInstanceHealthy(standbyInfo.getPersisted().getAdapterIdentifier());
                    }
                } catch (Exception e) {
                    log.error("Error while starting standby. It will be removed.");
                    platformAdapter.get().deletePostgresInstance(standbyInfo.getAdapter().getAdapterInstanceId());
                    postgresPersistedProperties.deletePostgresNodeInfo(standbyInfo.getPersisted().getInstanceId());
                }
            }
        }

        orchestratorUtils.getCombinedInfosForAvailableInstancesAsStream()
                .filter(info -> info.getAdapter().isActive() && InstanceHealth.HEALTHY.equals(info.getAdapter().getHealth()))
                .forEach(orchestratorUtils::addInstanceToRuntimePropertiesAndNotifyAllIfStandby);

        postgresConfigurator.initialize();

        validateDefaultSettingsPresence();

        log.info("Orchestrator initialization completed!");

        orchestratorReady.set(true);
    }

    @Override
    public void shutdown() {
        orchestratorReady.set(false);
        waitForActiveOperationsToComplete();
        postgresPersistedProperties.getPostgresNodeInfos().forEach(info ->
                platformAdapter.get().stopPostgresInstance(info.getAdapterIdentifier())
        );
        platformAdapter.get().shutdown();
    }

    @Override
    public boolean switchover(UUID newPrimaryInstanceId) throws OrchestratorNotReadyException, OrchestratorNotFoundException, OrchestratorOperationExecutionException {
        if (orchestratorReady.get()) {
            try {
                PostgresCombinedInstanceInfo newPrimaryInstanceInfo = orchestratorUtils.getCombinedInstanceInfo(newPrimaryInstanceId);
                PostgresCombinedInstanceInfo currentPrimaryInstanceInfo = orchestratorUtils.getCombinedInstanceInfo(clusterRuntimeProperties.getPrimaryInstanceInfo().getInstanceId());

                if (newPrimaryInstanceInfo == null) {
                    throw new OrchestratorNotFoundException("Instance with id '" + newPrimaryInstanceId + "' not found in persisted settings.");
                }

                return switchover(
                        newPrimaryInstanceInfo,
                        currentPrimaryInstanceInfo
                );
            } catch (OrchestratorNotFoundException e) {
                throw e;
            } catch (Exception e) {
                throw new OrchestratorOperationExecutionException("Failed to switchover ", e);
            }
        } else {
            throw new OrchestratorNotReadyException("Can not switchover. Orchestrator not ready. Its initialization is still in progress or PgFacade is configured to work just like proxy.");
        }
    }

    @Override
    public void changePostgresSettings(Map<String, String> newSettingNamesAndValuesMap) throws OrchestratorNotReadyException, OrchestratorOperationExecutionException {
        if (!orchestratorReady.get()) {
            throw new OrchestratorNotReadyException("Can not update Postgres settings. Orchestrator not ready. Its initialization is still in progress or PgFacade is configured to work just like proxy.");
        }

        if (switchoverInProgress.get()) {
            throw new OrchestratorOperationExecutionException("Switchover in progress. For safety reasons, it is impossible to change settings now. Try again later when switchover completed.");
        }

        boolean restartRequired = postgresConfigurator.validateSettingAndCheckIfRestartRequired(newSettingNamesAndValuesMap);

        if (!restartRequired) {
            try {
                orchestratorUtils.getCombinedInfosForAvailableInstancesAsStream()
                        .forEach(info -> postgresConfigurator.changePostgresSettings(info, newSettingNamesAndValuesMap));
            } catch (Exception e) {
                throw new OrchestratorOperationExecutionException("Unexpected error while updating Postgres settings.", e);
            }
        } else {
            List<PostgresCombinedInstanceInfo> combinedInstanceInfos = orchestratorUtils.getCombinedInfosForAvailableInstances();

            List<PostgresCombinedInstanceInfo> availableStandbys = combinedInstanceInfos
                    .stream()
                    .filter(info -> !info.getPersisted().isPrimary())
                    .filter(info -> info.getAdapter().isActive() && InstanceHealth.HEALTHY.equals(info.getAdapter().getHealth()))
                    .toList();

            PostgresCombinedInstanceInfo primaryCombinedInstanceInfo = combinedInstanceInfos
                    .stream()
                    .filter(info -> info.getPersisted().isPrimary())
                    .findFirst()
                    .orElse(null);

            if (CollectionUtils.isEmpty(availableStandbys)) {
                throw new OrchestratorOperationExecutionException("Provided settings require restart but currently there are no healthy standby nodes. For safety reasons, parameters can not be changed now. Try again later, when there will be at least one standby. However, it is highly recommended to change provided settings with at least 2 active standby nodes.");
            }

            if (primaryCombinedInstanceInfo == null) {
                throw new OrchestratorOperationExecutionException("Can not found primary. Is cluster recovering or lost?");
            }

            if (availableStandbys.size() == 1) {
                log.warn("Settings require restart but there is only 1 healthy standby. Potentially unsafe operation.");
            }

            if (!switchoverInProgress.compareAndSet(false, true)) {
                throw new OrchestratorOperationExecutionException("Can not change Postgres settings. Switchover in progress.");
            }

            try {
                log.warn("RESTARTING CLUSTER DUE TO POSTGRES SETTINGS CHANGES. CLUSTER WILL BE TEMPORARY UNAVAILABLE.");

                // using switchover event because for application restart looks the same
                UUID switchoverEventId = UUID.randomUUID();
                switchoverStartedEvent.fire(new SwitchoverStartedEvent(switchoverEventId));

                try {
                    postgresConfigurator.changePostgresSettings(primaryCombinedInstanceInfo, newSettingNamesAndValuesMap);
                    platformAdapter.get().restartPostgresInstance(primaryCombinedInstanceInfo.getAdapter().getAdapterInstanceId());

                    waitUntilPostgresInstanceHealthy(primaryCombinedInstanceInfo.getAdapter().getAdapterInstanceId());

                } catch (Exception e) {
                    // most likely we faced config parameter issue, so primary can not start.
                    // Because of that, there is no ability to revert settings (for non-running instance), so instance must be deleted
                    platformAdapter.get().deletePostgresInstance(primaryCombinedInstanceInfo.getAdapter().getAdapterInstanceId());
                    log.error("CLUSTER FAILED TO RESTART. WILL TRY TO RECOVER.");
                    switchoverCompletedEvent.fire(new SwitchoverCompletedEvent(switchoverEventId, false));
                    throw e;
                }

                log.warn("PRIMARY RESTARTED SUCCESSFULLY AND NEW POSTGRES SETTINGS WERE APPLIED. PRIMARY IS AVAILABLE NOW.");
                switchoverCompletedEvent.fire(new SwitchoverCompletedEvent(switchoverEventId, true));

                // all good for primary, so it will be good for every other Postgres instance
                for (PostgresCombinedInstanceInfo availableStandby : availableStandbys) {
                    postgresConfigurator.changePostgresSettings(availableStandby, newSettingNamesAndValuesMap);
                    restartStandbyAndWaitUntilItIsReadyAndRemoveOnFail(availableStandby);
                }
                log.warn("CLUSTER RESTARTED SUCCESSFULLY AND NEW POSTGRES SETTINGS WERE APPLIED. CLUSTER IS AVAILABLE NOW.");
            } catch (Exception e) {
                throw new OrchestratorOperationExecutionException("Failed to update Postgres settings", e);
            } finally {
                switchoverInProgress.set(false);
            }
        }

        postgresPersistedProperties.savePostgresSettingsInfos(newSettingNamesAndValuesMap);
    }

    @Scheduled(every = "${pg-facade.orchestration.common.postgres-dead-check.interval}")
    public void checkPrimaryLiveliness() {
        if (orchestratorReady.get() && !switchoverInProgress.get() && livelinessCheckInProgress.compareAndSet(false, true)) {
            try {
                checkPrimaryHealthAndFailoverIfNeeded();
            } finally {
                livelinessCheckInProgress.set(false);
            }
        }
    }

    @Scheduled(every = "${pg-facade.orchestration.common.standby.count-check-interval}")
    public void checkStandbyCount() {
        if (orchestratorReady.get() && !primaryUnhealthy.get() && !switchoverInProgress.get() && standbyCountCheckInProgress.compareAndSet(false, true)) {
            try {
                checkAndFixStandbyCount(orchestratorUtils.getCombinedInfosForAvailableInstances());
            } finally {
                standbyCountCheckInProgress.set(false);
            }
        }
    }

    private PostgresCombinedInstanceInfo restoreClusterFromBackup(boolean initializeArchiver) {
        if (initializeArchiver) {
            postgresArchiver.initialize();
        }

        log.info("Started restoring cluster from backup.");
        if (CollectionUtils.isEmpty(postgresArchiver.getBackupInstants())) {
            log.error("No backups are available in archive storage! Can not restore cluster from backup!");
            return null;
        } else {
            try {
                log.info("Restoring primary from backup. This will take some time...");
                try {
                    postgresPersistedProperties.getPostgresNodeInfos().forEach(info -> platformAdapter.get().deletePostgresInstance(info.getAdapterIdentifier()));
                } catch (Exception ignored) {
                }

                postgresPersistedProperties.clearPostgresNodesInfos();
                String newPrimaryAdapterId = postgresArchiver.restorePostgresToLatestVersionFromArchive();
                platformAdapter.get().startPostgresInstance(newPrimaryAdapterId);
                PostgresAdapterInstanceInfo primaryAdapterInstanceInfo = waitUntilPostgresInstanceHealthy(newPrimaryAdapterId);

                PostgresPersistedInstanceInfo persistedInstanceInfo = PostgresPersistedInstanceInfo
                        .builder()
                        .primary(true)
                        .instanceId(UUID.randomUUID())
                        .adapterIdentifier(primaryAdapterInstanceInfo.getAdapterInstanceId())
                        .build();

                postgresPersistedProperties.savePostgresNodeInfo(persistedInstanceInfo);

                PostgresCombinedInstanceInfo combinedInstanceInfo = PostgresCombinedInstanceInfo
                        .builder()
                        .persisted(persistedInstanceInfo)
                        .adapter(primaryAdapterInstanceInfo)
                        .build();

                orchestratorUtils.addInstanceToRuntimePropertiesAndNotifyAllIfStandby(combinedInstanceInfo);

                return combinedInstanceInfo;

            } catch (Exception e) {
                log.error("Failed to restore from backup! Restore manually and restart PgFacade in 'RECOVERY' mode!", e);
                return null;
            }
        }
    }

    private void validateDefaultSettingsPresence() {
        Map<String, String> defaultSettings = postgresUtils.getDefaultSettings(clusterRuntimeProperties.getPostgresVersion());

        Map<String, String> persistedSettings = postgresPersistedProperties.getPostgresSettingInfos();
        Map<String, String> mergedSettings = new HashMap<>(persistedSettings);

        defaultSettings.forEach(mergedSettings::putIfAbsent);

        if (!persistedSettings.equals(mergedSettings)) {
            log.info("Not all required settings have values. Will apply default values.");
            try {
                orchestratorUtils.getCombinedInfosForAvailableInstancesAsStream()
                        .forEach(
                                instance -> postgresConfigurator.changePostgresSettings(instance, mergedSettings)
                        );
                postgresPersistedProperties.savePostgresSettingsInfos(mergedSettings);
            } catch (Exception e) {
                log.error("Failed to apply required settings default values ", e);
            }
        }
    }

    private void checkPrimaryHealthAndFailoverIfNeeded() {
        List<PostgresCombinedInstanceInfo> availableInstances = orchestratorUtils.getCombinedInfosForAvailableInstances();

        boolean healthcheckFailed = false;
        PostgresCombinedInstanceInfo newPrimaryInstanceInfo = null;

        PostgresCombinedInstanceInfo currentPrimary = availableInstances.stream()
                .filter(info -> info.getPersisted().isPrimary())
                .findFirst()
                .orElse(null);

        if (currentPrimary == null) {
            healthcheckFailedCount = Integer.MAX_VALUE;
            log.error("CAN NOT FIND POSTGRES PRIMARY. HEALTHCHECK FAILED!");
        } else {
            if (!currentPrimary.getAdapter().isActive() || !InstanceHealth.HEALTHY.equals(currentPrimary.getAdapter().getHealth())) {
                healthcheckFailedCount++;
                healthcheckFailed = true;
                primaryUnhealthy.set(true);
                log.error("POSTGRES PRIMARY UNHEALTHY. {} HEALTHCHECKS FAILED WHEN MAXIMUM IS {}", healthcheckFailedCount, orchestrationProperties.common().postgresDeadCheck().retries());
            }
        }

        if (healthcheckFailedCount >= orchestrationProperties.common().postgresDeadCheck().retries()) {
            log.error("REACHED MAXIMUM HEALTHCHECKS RETRY COUNT. NEED TO FAILOVER.");

            // mark orchestrator as not ready, to prevent any other changes during failover
            orchestratorReady.set(false);

            newPrimaryInstanceInfo = selectNewPrimary(availableInstances);
            if (newPrimaryInstanceInfo == null) {
                log.error("FATAL ERROR. POSTGRES PRIMARY IS UNHEALTHY BUT NO ALIVE AND ACTIVE STANDBY FOUND. CAN NOT FAILOVER IMMIDIATLY!");

                if (!orchestrationProperties.postgresClusterRestore().autoRestoreLostCluster()) {
                    log.error("WILL NOT TRY TO RESTORE CLUSTER FROM BACKUP, BECAUSE CONFIGURATION PROHIBITS THIS ACTION! RESTORE PRIMARY MANUALLY AND RESTART PGFACADE IN 'RECOVERY' MODE!");
                    // keep orchestrator inactive.
                    return;
                } else {
                    UUID switchoverEventId = UUID.randomUUID();

                    log.error("RESTORING PRIMARY USING LATEST AVAILABLE VERSION FROM ARCHIVE.");
                    switchoverStartedEvent.fire(new SwitchoverStartedEvent(switchoverEventId));

                    PostgresCombinedInstanceInfo combinedInstanceInfo = restoreClusterFromBackup(false);
                    if (combinedInstanceInfo == null) {
                        switchoverCompletedEvent.fire(new SwitchoverCompletedEvent(switchoverEventId, false));
                        log.error("FAILED TO RESTORE PRIMARY FROM BACKUP!");
                        orchestratorReady.set(true);
                        return;
                    }

                    switchoverCompletedEvent.fire(new SwitchoverCompletedEvent(switchoverEventId, true));

                    log.info("SUCCESFULY RESTORED LOST CLUSTER FROM BACKUP!");
                    healthcheckFailedCount = 0;
                }
            }
        }

        if (newPrimaryInstanceInfo != null) {
            log.info("FAILOVER STARTED. STANDBY WILL SWITCHOVER PRIMARY.");

            if (!switchover(newPrimaryInstanceInfo, currentPrimary)) {
                log.error("FATAL ERROR. TRIED TO PROMOTE STANDBY BUT FAILED. FAILOVER FAILED!!!");
                orchestratorReady.set(true);
                return;
            } else {
                try {
                    waitUntilPostgresInstanceHealthy(newPrimaryInstanceInfo.getAdapter().getAdapterInstanceId());
                } catch (Exception e) {
                    log.error("FAILED TO ACHIEVE HEALTH PRIMARY AFTER SWITCHOVER!");
                    orchestratorReady.set(true);
                    return;
                }
                healthcheckFailedCount = 0;
            }
        }

        if (!healthcheckFailed) {
            healthcheckFailedCount = 0;
            primaryUnhealthy.set(false);
        }

        orchestratorReady.set(true);
    }

    private PostgresCombinedInstanceInfo selectNewPrimary(List<PostgresCombinedInstanceInfo> availableInstances) {
        //TODO maybe add some better logic to select new primary. By LSN?
        return availableInstances
                .stream()
                .filter(info ->
                        info.getAdapter().isActive()
                                && Boolean.FALSE.equals(info.getPersisted().isPrimary())
                                && InstanceHealth.HEALTHY.equals(info.getAdapter().getHealth())
                )
                .filter(info -> !restartingStandbyInstanceIds.contains(info.getPersisted().getInstanceId()))
                .findFirst()
                .orElse(null);
    }

    private void checkAndFixStandbyCount(List<PostgresCombinedInstanceInfo> availableInstances) {
        List<PostgresCombinedInstanceInfo> healthyOrStartingStandby = availableInstances
                .stream()
                .filter(info -> Boolean.FALSE.equals(info.getPersisted().isPrimary()))
                .filter(info ->
                        InstanceHealth.HEALTHY.equals(info.getAdapter().getHealth())
                                || InstanceHealth.STARTING.equals(info.getAdapter().getHealth())
                )
                .toList();

        long countDiff = orchestrationProperties.common().standby().count() - healthyOrStartingStandby.size();

        if (healthyOrStartingStandby.size() < orchestrationProperties.common().standby().count()) {
            log.warn("Found {} starting or healthy standby while it is required to have {}. Need to start {} more.",
                    healthyOrStartingStandby.size(),
                    orchestrationProperties.common().standby().count(),
                    countDiff
            );

            List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();

            for (int i = 0; i < countDiff; i++) {
                completableFutureList.add(managedExecutor.runAsync(() -> {
                            PostgresCombinedInstanceInfo instanceInfo = createStartAndWaitForNewInstanceToBeReady(false);
                            orchestratorUtils.addInstanceToRuntimePropertiesAndNotifyAllIfStandby(instanceInfo);
                            log.info("Standby is up and running!");
                        })
                );
            }

            try {
                CompletableFuture.allOf(completableFutureList.toArray(new CompletableFuture[0])).join();
            } catch (Exception e) {
                log.error("Error while scaling standby count", e);
            }
        } else if (healthyOrStartingStandby.size() > orchestrationProperties.common().standby().count()) {
            log.warn("Found {} starting or healthy standby while it is required to have {}. Will stop {} instance(s).",
                    healthyOrStartingStandby.size(),
                    orchestrationProperties.common().standby().count(),
                    Math.abs(countDiff)
            );

            for (int i = 0; i < Math.abs(countDiff); i++) {
                platformAdapter.get().deletePostgresInstance(healthyOrStartingStandby.get(i).getAdapter().getAdapterInstanceId());
            }
        }

        //remove unhealthy or inactive standby
        availableInstances
                .stream()
                .filter(info -> !restartingStandbyInstanceIds.contains(info.getPersisted().getInstanceId()))
                .filter(info -> Boolean.FALSE.equals(info.getPersisted().isPrimary()))
                .filter(info ->
                        !info.getAdapter().isActive()
                                || InstanceHealth.UNHEALTHY.equals(info.getAdapter().getHealth())

                )
                .forEach(info -> {
                    log.info("Found unhealthy or inactive standby. Removing it.");
                    postgresPersistedProperties.deletePostgresNodeInfo(info.getPersisted().getInstanceId());
                    removeStandby(info);
                });
    }

    private void removeStandby(PostgresCombinedInstanceInfo standbyInstanceInfo) {
        standbyRemovedEvent.fire(
                new StandbyRemovedEvent(
                        standbyInstanceInfo.getPersisted().getInstanceId()
                )
        );

        clusterRuntimeProperties.getAllPostgresInstancesInfos().remove(standbyInstanceInfo.getPersisted().getInstanceId());
        postgresPersistedProperties.deletePostgresNodeInfo(standbyInstanceInfo.getPersisted().getInstanceId());
        platformAdapter.get().deletePostgresInstance(standbyInstanceInfo.getAdapter().getAdapterInstanceId());
    }

    private synchronized boolean switchover(PostgresCombinedInstanceInfo newPrimaryInstanceInfo, PostgresCombinedInstanceInfo currentPrimaryInstanceInfo) {
        UUID switchoverEventId = UUID.randomUUID();

        switchoverInProgress.set(true);

        try {
            switchoverStartedEvent.fire(new SwitchoverStartedEvent(switchoverEventId));
            standbyRemovedEvent.fireAsync(new StandbyRemovedEvent(newPrimaryInstanceInfo.getPersisted().getInstanceId()));
            log.info("SWITCHOVER STARTED. SWITCHING TO INSTANCE WITH IP {} AND PORT {}", newPrimaryInstanceInfo.getAdapter().getInstanceAddress(), newPrimaryInstanceInfo.getAdapter().getInstancePort());

            Connection standbyConnection = postgresUtils.getConnectionForPgFacadeUser(
                    newPrimaryInstanceInfo.getAdapter().getInstanceAddress(),
                    newPrimaryInstanceInfo.getAdapter().getInstancePort()
            );

            ResultSet promoteResultSet = standbyConnection.createStatement().executeQuery("SELECT pg_promote()");
            promoteResultSet.next();
            boolean promoteSuccessful = promoteResultSet.getBoolean(1);
            if (!promoteSuccessful) {
                log.error("ERROR DURING SWITCHOVER. STANDBY CANT COMPLETE PROMOTE REQUEST.");
                return false;
            }

            standbyConnection.close();

            clusterRuntimeProperties.getAllPostgresInstancesInfos().remove(currentPrimaryInstanceInfo.getPersisted().getInstanceId());

            newPrimaryInstanceInfo.getPersisted().setPrimary(true);
            postgresPersistedProperties.savePostgresNodeInfo(newPrimaryInstanceInfo.getPersisted());
            orchestratorUtils.addInstanceToRuntimePropertiesAndNotifyAllIfStandby(newPrimaryInstanceInfo);

            platformAdapter.get().deletePostgresInstance(currentPrimaryInstanceInfo.getAdapter().getAdapterInstanceId());
            postgresPersistedProperties.deletePostgresNodeInfo(currentPrimaryInstanceInfo.getPersisted().getInstanceId());

            //remove all standby because of new timeline
            //TODO it is possible to repair such nodes instead of deleting them. Use recovery_target_timeline = 'latest'
            log.info("NEW PRIMARY PROMOTED. REMOVING ALL FORMER STANDBY BECAUSE OF NEW TIMELINE.");
            orchestratorUtils.getCombinedInfosForStandbyInstances().forEach(this::removeStandby);

            switchoverCompletedEvent.fire(new SwitchoverCompletedEvent(switchoverEventId, true));
            log.info("SWITCHOVER COMPLETED SUCCESSFULLY");
            return true;
        } catch (Exception e) {
            switchoverCompletedEvent.fire(new SwitchoverCompletedEvent(switchoverEventId, false));
            log.error("ERROR DURING SWITCHOVER", e);
            return false;
        } finally {
            switchoverInProgress.set(false);
        }
    }

    private void restartStandbyAndWaitUntilItIsReadyAndRemoveOnFail(PostgresCombinedInstanceInfo standbyInfo) {
        UUID instanceId = standbyInfo.getPersisted().getInstanceId();
        restartingStandbyInstanceIds.add(instanceId);
        orchestratorUtils.removeInstanceFromRuntimePropertiesAndNotifyAllIfStandby(instanceId);

        try {
            platformAdapter.get().restartPostgresInstance(standbyInfo.getAdapter().getAdapterInstanceId());

            PostgresAdapterInstanceInfo restartedAdapterInstanceInfo = waitUntilPostgresInstanceHealthy(standbyInfo.getAdapter().getAdapterInstanceId());

            PostgresCombinedInstanceInfo newInstanceInfo = PostgresCombinedInstanceInfo
                    .builder()
                    .adapter(restartedAdapterInstanceInfo)
                    .persisted(standbyInfo.getPersisted())
                    .build();

            orchestratorUtils.addInstanceToRuntimePropertiesAndNotifyAllIfStandby(newInstanceInfo);

        } catch (Exception e) {
            log.error("Error while restarting standby! ", e);
            restartingStandbyInstanceIds.remove(instanceId);
            postgresPersistedProperties.deletePostgresNodeInfo(instanceId);
            platformAdapter.get().deletePostgresInstance(standbyInfo.getAdapter().getAdapterInstanceId());
        }
    }

    private PostgresCombinedInstanceInfo createStartAndWaitForNewInstanceToBeReady(boolean primary) throws InstanceCreationException, AwaitHealthyInstanceException {
        UUID futureInstanceId = UUID.randomUUID();
        String adapterIdentifier = platformAdapter.get().createNewPostgresInstance(
                PostgresInstanceCreationRequest
                        .builder()
                        .futureInstanceId(UUID.randomUUID())
                        .primary(primary)
                        .settings(postgresPersistedProperties.getPostgresSettingInfos())
                        .build()
        );

        if (adapterIdentifier == null) {
            throw new InstanceCreationException("Can not create new Postgres instance.");
        }

        boolean started = platformAdapter.get().startPostgresInstance(adapterIdentifier);

        if (!started) {
            throw new InstanceCreationException("Can not start new Postgres instance");
        }

        log.info("Started new instance. Will wait until it is healthy...");

        PostgresAdapterInstanceInfo adapterInstanceInfo = waitUntilPostgresInstanceHealthy(adapterIdentifier);

        PostgresPersistedInstanceInfo persistedInstanceInfo = PostgresPersistedInstanceInfo
                .builder()
                .primary(primary)
                .instanceId(futureInstanceId)
                .adapterIdentifier(adapterIdentifier)
                .build();

        postgresPersistedProperties.savePostgresNodeInfo(persistedInstanceInfo);

        return PostgresCombinedInstanceInfo
                .builder()
                .adapter(adapterInstanceInfo)
                .persisted(persistedInstanceInfo)
                .build();
    }

    private PostgresAdapterInstanceInfo waitUntilPostgresInstanceHealthy(String adapterInstanceId) throws AwaitHealthyInstanceException {
        OrchestrationProperties.CommonProperties.PostgresStartupCheckProperties startupCheckProperties = orchestrationProperties.common().postgresStartupCheck();

        long endTime = System.currentTimeMillis() + (startupCheckProperties.interval() * startupCheckProperties.retries()) + startupCheckProperties.startPeriod();
        PostgresAdapterInstanceInfo instanceInfo = platformAdapter.get().getInstanceInfo(adapterInstanceId);

        try {
            while (!InstanceHealth.HEALTHY.equals(instanceInfo.getHealth()) && endTime > System.currentTimeMillis()) {
                Thread.sleep(startupCheckProperties.interval());
                instanceInfo = platformAdapter.get().getInstanceInfo(adapterInstanceId);
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new AwaitHealthyInstanceException("Failed to achieve healthy instance.", interruptedException);
        }

        if (!InstanceHealth.HEALTHY.equals(instanceInfo.getHealth())) {
            throw new AwaitHealthyInstanceException("Failed to achieve healthy instance. Timout reached.");
        }

        return instanceInfo;
    }

    private void waitForActiveOperationsToComplete() {
        while (livelinessCheckInProgress.get() || switchoverInProgress.get() || standbyCountCheckInProgress.get()) {
            // waiting for all operations to complete.
        }
    }
}
