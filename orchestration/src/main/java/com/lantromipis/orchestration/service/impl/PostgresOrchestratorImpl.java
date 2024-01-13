package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.event.SwitchoverCompletedEvent;
import com.lantromipis.configuration.event.SwitchoverStartedEvent;
import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.producers.RuntimePostgresConnectionProducer;
import com.lantromipis.configuration.properties.constant.PostgresConstants;
import com.lantromipis.configuration.properties.predefined.ArchivingProperties;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.configuration.properties.runtime.PostgresSettingsRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.*;
import com.lantromipis.orchestration.model.*;
import com.lantromipis.orchestration.model.raft.PostgresPersistedInstanceInfo;
import com.lantromipis.orchestration.service.api.*;
import com.lantromipis.orchestration.util.OrchestratorUtils;
import com.lantromipis.orchestration.util.PostgresUtils;
import com.lantromipis.orchestration.util.RaftFunctionalityCombinator;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.eclipse.microprofile.context.ManagedExecutor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
    PostgresConfigurator postgresConfigurator;

    @Inject
    ManagedExecutor managedExecutor;

    @Inject
    PostgresUtils postgresUtils;

    @Inject
    ArchivingProperties archivingProperties;

    @Inject
    PostgresArchiver postgresArchiver;

    @Inject
    PostgresRestorationService postgresRestorationService;

    @Inject
    OrchestratorUtils orchestratorUtils;

    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    @Inject
    RaftFunctionalityCombinator raftFunctionalityCombinator;

    @Inject
    Instance<PostgresHealthcheckService> postgresHealthcheckService;

    @Inject
    RuntimePostgresConnectionProducer runtimePostgresConnectionProducer;

    @Inject
    PostgresSettingsRuntimeProperties postgresSettingsRuntimeProperties;

    private final AtomicBoolean orchestratorReady = new AtomicBoolean(false);
    private final AtomicBoolean livelinessCheckInProgress = new AtomicBoolean(false);
    private final AtomicBoolean standbyCountCheckInProgress = new AtomicBoolean(false);
    private final AtomicBoolean switchoverInProgress = new AtomicBoolean(false);
    private final AtomicBoolean primaryUnhealthy = new AtomicBoolean(false);
    private int healthcheckFailedCount = 0;
    private final Set<UUID> restartingStandbyInstanceIds = ConcurrentHashMap.newKeySet();

    private final static long HEALTHCHECK_TIMEOUT = 1500;
    private final Map<UUID, Instant> newlyCreatedStartingStandbys = new ConcurrentHashMap<>();

    @Override
    public void initializeFastWhenClusterRunning() throws Exception {
        if (PgFacadeRaftRole.FOLLOWER.equals(pgFacadeRuntimeProperties.getRaftRole())) {
            log.info("Not starting Postgres orchestration because this PgFacade instance is not current raft leader.");
            postgresSettingsRuntimeProperties.reload();

            if (archivingProperties.enabled()) {
                postgresArchiver.initialize();
            }
            log.info("Fast orchestrator initialization completed!");
            return;
        }

        if (archivingProperties.enabled()) {
            postgresArchiver.startArchiving();
        } else {
            log.warn("Archiving is disabled. Continuous Archiving and Point-in-Time Recovery will not be possible!");
        }
        orchestratorUtils.getCombinedInfosForAvailableInstancesAsStream().forEach(
                instanceInfo -> orchestratorUtils.addInstanceToRuntimePropertiesAndNotifyAllIfStandby(instanceInfo)
        );

        orchestratorReady.set(true);
        postgresSettingsRuntimeProperties.reload();
        log.info("Fast orchestrator initialization completed!");
    }

    @Override
    public void initializeFull() throws Exception {
        if (PgFacadeRaftRole.FOLLOWER.equals(pgFacadeRuntimeProperties.getRaftRole())) {
            log.info("Not starting Postgres orchestration because this PgFacade instance is not current raft leader.");
            postgresSettingsRuntimeProperties.reload();
            return;
        }

        PostgresPersistedInstanceInfo primaryPersistedInstanceInfo = raftFunctionalityCombinator.getPostgresNodeInfos()
                .stream()
                .filter(PostgresPersistedInstanceInfo::isPrimary)
                .findFirst()
                .orElse(null);

        PostgresAdapterInstanceInfo adapterInstanceInfo = null;
        boolean primaryFoundAndStarted = false;

        if (primaryPersistedInstanceInfo != null) {
            adapterInstanceInfo = platformAdapter.get().getPostgresInstanceInfo(primaryPersistedInstanceInfo.getAdapterIdentifier());

            if (adapterInstanceInfo.isActive()) {
                primaryFoundAndStarted = true;
                log.info("Found active Postgres primary. No actions needed.");

            } else {
                log.info("Found non-active Postgres primary instance. Will start it now.");

                try {
                    platformAdapter.get().startPostgresInstance(adapterInstanceInfo.getAdapterInstanceId());
                    adapterInstanceInfo = waitUntilPostgresInstanceHealthy(adapterInstanceInfo.getAdapterInstanceId());
                    if (adapterInstanceInfo == null) {
                        log.error("Failed to start non-active Postgres primary!");
                    } else {
                        primaryFoundAndStarted = true;
                    }
                    log.info("Successfully started non-active Postgres primary!");
                } catch (Exception e) {
                    log.error("Failed to start non-active Postgres primary!", e);
                }
            }
        }

        if (!primaryFoundAndStarted) {
            throw new NoPrimaryException("Postgres primary NOT found! Con not start orchestration!");
        }

        PostgresCombinedInstanceInfo primaryInstanceInfo = PostgresCombinedInstanceInfo
                .builder()
                .adapter(adapterInstanceInfo)
                .persisted(primaryPersistedInstanceInfo)
                .build();

        orchestratorUtils.addInstanceToRuntimePropertiesAndNotifyAllIfStandby(primaryInstanceInfo);
        postgresSettingsRuntimeProperties.reload();

        log.info("Primary is up and running!");

        // standby section
        List<PostgresCombinedInstanceInfo> standbyInfos = orchestratorUtils.getCombinedInfosForStandbyInstances();

        log.info("Checking if Postgres standby instances are active.");

        for (PostgresCombinedInstanceInfo standbyInfo : standbyInfos) {
            if (standbyInfo.getAdapter().isActive()) {
                log.info("Known standby with name {} is already running!", standbyInfo.getPersisted().getServerName());
                boolean healthCheckSucceeded = postgresHealthcheckService.get().checkPostgresLiveliness(
                        standbyInfo.getAdapter().getInstanceAddress(),
                        standbyInfo.getAdapter().getInstancePort(),
                        HEALTHCHECK_TIMEOUT
                );

                if (!healthCheckSucceeded) {
                    log.error("Failed to healthcheck running standby. Will remove it");
                    raftFunctionalityCombinator.deletePostgresNodeInfoInRaft(standbyInfo.getPersisted().getInstanceId());
                    platformAdapter.get().deleteInstance(standbyInfo.getAdapter().getAdapterInstanceId());
                } else {
                    orchestratorUtils.addInstanceToRuntimePropertiesAndNotifyAllIfStandby(standbyInfo);
                }
                continue;
            }

            log.info("Found inactive standby with name {}. Will start it now.", standbyInfo.getPersisted().getServerName());
            PostgresAdapterInstanceInfo standbyAdapterInstanceInfo = startPostgresInstanceAndWaitToBeReady(standbyInfo.getPersisted().getAdapterIdentifier());
            if (standbyAdapterInstanceInfo == null) {
                log.error("Failed to start inactive standby with name {}. Will remove it.", standbyInfo.getPersisted().getServerName());
                raftFunctionalityCombinator.deletePostgresNodeInfoInRaft(standbyInfo.getPersisted().getInstanceId());
                platformAdapter.get().deleteInstance(standbyInfo.getAdapter().getAdapterInstanceId());
                continue;
            }

            boolean configuredSuccessfully = configureStandbyForReplication(
                    standbyAdapterInstanceInfo.getAdapterInstanceId(),
                    standbyAdapterInstanceInfo.getInstanceAddress(),
                    standbyAdapterInstanceInfo.getInstancePort(),
                    standbyInfo.getPersisted().getServerName(),
                    standbyInfo.getPersisted().getReplicationSlotName()
            );

            if (!configuredSuccessfully) {
                log.error("Failed to configure standby with name {}. Will remove it.", standbyInfo.getPersisted().getServerName());
                raftFunctionalityCombinator.deletePostgresNodeInfoInRaft(standbyInfo.getPersisted().getInstanceId());
                platformAdapter.get().deleteInstance(standbyInfo.getAdapter().getAdapterInstanceId());
            } else {
                orchestratorUtils.addInstanceToRuntimePropertiesAndNotifyAllIfStandby(standbyInfo);
            }
        }

        if (archivingProperties.enabled()) {
            postgresArchiver.initialize();
            postgresArchiver.startArchiving();
        } else {
            log.warn("Archiving is disabled. Continuous Archiving and Point-in-Time Recovery will not be possible!");
        }

        log.info("Orchestrator initialization completed!");

        orchestratorReady.set(true);
    }

    @Override
    public void stopOrchestrator(boolean shutdownPostgres) {
        orchestratorReady.set(false);
        postgresArchiver.stop();
        waitForActiveOperationsToComplete();

        if (shutdownPostgres) {
            orchestratorUtils.getCombinedInfosForAvailableInstances()
                    .forEach(instance -> {
                                try {
                                    platformAdapter.get().stopPostgresInstance(instance.getAdapter().getAdapterInstanceId());
                                } catch (Exception e) {
                                    //ignored
                                }
                            }
                    );
        }
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
    public PostgresClusterSettingsChangeResult changePostgresSettings(Map<String, String> newSettingNamesAndValuesMap) throws OrchestratorNotReadyException, OrchestratorOperationExecutionException {
        if (!orchestratorReady.get()) {
            return PostgresClusterSettingsChangeResult
                    .builder()
                    .status(PostgresClusterSettingsChangeResult.Status.NOT_READY_ERROR)
                    .build();
        }

        if (switchoverInProgress.get()) {
            log.warn("Settings change request was received, but switchover is in progress");
            return PostgresClusterSettingsChangeResult
                    .builder()
                    .status(PostgresClusterSettingsChangeResult.Status.CLUSTER_MODIFICATION_IN_PROGRESS_ERROR)
                    .build();
        }

        PostgresSettingsValidationResult validationResult = postgresConfigurator.validateSettingAndCheckIfRestartRequired(newSettingNamesAndValuesMap);
        if (!validationResult.isSettingsValid()) {
            return PostgresClusterSettingsChangeResult
                    .builder()
                    .status(PostgresClusterSettingsChangeResult.Status.VALIDATION_ERROR)
                    .settingNameToValidationError(validationResult.getSettingNameToError())
                    .build();
        }

        // in case no restart required, just update instances one by one
        if (!validationResult.isRestartRequired()) {
            // change standby first
            List<PostgresCombinedInstanceInfo> combinedInstanceInfos = orchestratorUtils.getCombinedInfosForAvailableInstancesAsStream()
                    .sorted((i1, i2) -> Boolean.compare(i1.getPersisted().isPrimary(), i2.getPersisted().isPrimary()))
                    .toList();

            for (int i = 0; i < combinedInstanceInfos.size(); i++) {
                PostgresCombinedInstanceInfo info = combinedInstanceInfos.get(i);
                PostgresInstanceSettingsChangeResult changeResult = postgresConfigurator.changePostgresInstanceSettingsAndRollbackOnFailure(
                        info.getPersisted().getInstanceId(),
                        newSettingNamesAndValuesMap
                );
                // do not continue if first fails
                if (!PostgresInstanceSettingsChangeResult.Status.SUCCESS.equals(changeResult.getStatus()) && i == 0) {
                    // delete if rollback failed
                    if (changeResult.isRollbackWasRequired() && CollectionUtils.isNotEmpty(changeResult.getNotRollbackedSettings())) {
                        platformAdapter.get().deleteInstance(info.getAdapter().getAdapterInstanceId());
                    }

                    return PostgresClusterSettingsChangeResult
                            .builder()
                            .status(PostgresClusterSettingsChangeResult.Status.SETTING_CHANGE_ERROR)
                            .settingNamesForWhichRollbackFailed(changeResult.getNotRollbackedSettings())
                            .settingChangeErrorMessage(changeResult.getSqlErrorMessage())
                            .build();
                }
            }
            return PostgresClusterSettingsChangeResult
                    .builder()
                    .status(PostgresClusterSettingsChangeResult.Status.SUCCESS)
                    .build();
        }

        // in case restart required, restart standby and only then primary
        List<PostgresCombinedInstanceInfo> combinedInstanceInfos = orchestratorUtils.getCombinedInfosForAvailableInstances();

        List<PostgresCombinedInstanceInfo> availableStandbys = combinedInstanceInfos
                .stream()
                .filter(info -> !info.getPersisted().isPrimary())
                .filter(info -> postgresHealthcheckService.get().checkPostgresLiveliness(info.getAdapter().getInstanceAddress(), info.getAdapter().getInstancePort(), HEALTHCHECK_TIMEOUT))
                .toList();

        if (CollectionUtils.isEmpty(availableStandbys)) {
            return PostgresClusterSettingsChangeResult
                    .builder()
                    .status(PostgresClusterSettingsChangeResult.Status.NO_AVAILABLE_STANDBY_ERROR)
                    .build();
        }

        PostgresCombinedInstanceInfo primaryCombinedInstanceInfo = combinedInstanceInfos
                .stream()
                .filter(info -> info.getPersisted().isPrimary())
                .findFirst()
                .orElse(null);

        if (primaryCombinedInstanceInfo == null) {
            return PostgresClusterSettingsChangeResult
                    .builder()
                    .status(PostgresClusterSettingsChangeResult.Status.NO_PRIMARY_ERROR)
                    .build();
        }

        if (availableStandbys.size() == 1) {
            log.warn("Settings require restart but there is only 1 healthy standby. Potentially unsafe operation.");
        }

        if (!switchoverInProgress.compareAndSet(false, true)) {
            return PostgresClusterSettingsChangeResult
                    .builder()
                    .status(PostgresClusterSettingsChangeResult.Status.CLUSTER_MODIFICATION_IN_PROGRESS_ERROR)
                    .build();
        }

        UUID switchoverEventId = UUID.randomUUID();

        try {
            log.warn("RESTARTING CLUSTER DUE TO POSTGRES SETTINGS CHANGES. CLUSTER WILL BE TEMPORARY UNAVAILABLE.");

            // using switchover event because for application restart looks the same
            raftFunctionalityCombinator.notifyAllClusterAboutSwitchoverStarted(new SwitchoverStartedEvent(switchoverEventId));

            PostgresInstanceSettingsChangeResult primaryChangeResult = postgresConfigurator.changePostgresInstanceSettingsAndRollbackOnFailure(
                    primaryCombinedInstanceInfo.getPersisted().getInstanceId(),
                    newSettingNamesAndValuesMap
            );

            if (!PostgresInstanceSettingsChangeResult.Status.SUCCESS.equals(primaryChangeResult.getStatus())) {
                // delete if rollback failed
                if (primaryChangeResult.isRollbackWasRequired() && CollectionUtils.isNotEmpty(primaryChangeResult.getNotRollbackedSettings())) {
                    log.error("FAILED TO ROLLBACK SOME POSTGRES PRIMARY SETTINGS! WILL REMOVE POSTGRES PRIMARY!");
                    platformAdapter.get().deleteInstance(primaryCombinedInstanceInfo.getAdapter().getAdapterInstanceId());
                }

                return PostgresClusterSettingsChangeResult
                        .builder()
                        .status(PostgresClusterSettingsChangeResult.Status.SETTING_CHANGE_ERROR)
                        .settingNamesForWhichRollbackFailed(primaryChangeResult.getNotRollbackedSettings())
                        .settingChangeErrorMessage(primaryChangeResult.getSqlErrorMessage())
                        .build();
            }

            platformAdapter.get().restartPostgresInstance(primaryCombinedInstanceInfo.getAdapter().getAdapterInstanceId());
            PostgresAdapterInstanceInfo adapterInstanceInfo = waitUntilPostgresInstanceHealthy(primaryCombinedInstanceInfo.getAdapter().getAdapterInstanceId());
            if (adapterInstanceInfo == null) {
                // most likely we faced config parameter issue, so primary can not start.
                // Because of that, there is no ability to revert settings (for non-running instance), so instance must be deleted
                platformAdapter.get().deleteInstance(primaryCombinedInstanceInfo.getAdapter().getAdapterInstanceId());
                log.error("PRIMARY FAILED TO RESTART AFTER SETTINGS WAS CHANGED. ARE NEW SETTINGS CORRECT? WILL TRY TO RECOVER.");
                return PostgresClusterSettingsChangeResult
                        .builder()
                        .status(PostgresClusterSettingsChangeResult.Status.RESTART_AFTER_CHANGE_ERROR)
                        .build();
            }

            log.info("PRIMARY RESTARTED SUCCESSFULLY AND NEW POSTGRES SETTINGS WERE APPLIED. PRIMARY IS AVAILABLE NOW.");

            // all good for primary, so it will 99% be good for every other Postgres instance
            for (PostgresCombinedInstanceInfo availableStandby : availableStandbys) {
                PostgresInstanceSettingsChangeResult standbyChangeResult = postgresConfigurator.changePostgresInstanceSettingsAndRollbackOnFailure(
                        availableStandby.getPersisted().getInstanceId(),
                        newSettingNamesAndValuesMap
                );
                // failed to rollback
                if (!PostgresInstanceSettingsChangeResult.Status.SUCCESS.equals(standbyChangeResult.getStatus())
                        && standbyChangeResult.isRollbackWasRequired()
                        && CollectionUtils.isNotEmpty(standbyChangeResult.getNotRollbackedSettings())) {
                    log.error("FAILED TO ROLLBACK INCORRECT SETTINGS FOR STANDBY!");
                    removeStandby(availableStandby);
                }
                // failed to restart
                boolean restartSuccessful = restartStandbyAndWaitUntilItIsReadyAndRemoveOnFail(availableStandby);
                if (!restartSuccessful) {
                    log.error("FAILED TO RESTART STANDBY AFTER SETTINGS WAS CHANGED!");
                }
            }
            log.info("CLUSTER RESTARTED SUCCESSFULLY AND NEW POSTGRES SETTINGS WERE APPLIED. CLUSTER IS AVAILABLE NOW.");

            try {
                raftFunctionalityCombinator.notifyClusterAboutSettingsChange();
            } catch (RaftException e) {
                throw new OrchestratorOperationExecutionException("Failed to save settings in Raft!", e);
            }

            return PostgresClusterSettingsChangeResult
                    .builder()
                    .status(PostgresClusterSettingsChangeResult.Status.SUCCESS)
                    .build();

        } catch (Exception e) {
            log.error("Failed to update Postgres settings", e);
            return PostgresClusterSettingsChangeResult
                    .builder()
                    .status(PostgresClusterSettingsChangeResult.Status.UNKNOWN_ERROR)
                    .build();
        } finally {
            switchoverInProgress.set(false);
            try {
                raftFunctionalityCombinator.notifyAllClusterAboutSwitchoverCompleted(new SwitchoverCompletedEvent(switchoverEventId, true));
            } catch (Exception e) {
                log.error("Failed to notify cluster about switchover completed!", e);
            }
        }
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
    @Blocking
    public void checkStandbyCount() {
        if (orchestratorReady.get() && !primaryUnhealthy.get() && !switchoverInProgress.get() && standbyCountCheckInProgress.compareAndSet(false, true)) {
            try {
                checkAndFixStandbyCount(orchestratorUtils.getCombinedInfosForAvailableInstances());
            } finally {
                standbyCountCheckInProgress.set(false);
            }
        }
    }

    //TODO remove
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
                    raftFunctionalityCombinator.getPostgresNodeInfos().forEach(info -> platformAdapter.get().deleteInstance(info.getAdapterIdentifier()));
                } catch (Exception ignored) {
                }

                raftFunctionalityCombinator.clearPostgresNodesInfosInRaft();
                String newPrimaryAdapterId = postgresRestorationService.stopArchiverAndRestorePostgresFromBackup();

                PostgresAdapterInstanceInfo primaryAdapterInstanceInfo = startPostgresInstanceAndWaitToBeReady(newPrimaryAdapterId);
                if (primaryAdapterInstanceInfo == null) {
                    log.error("Failed to restore Postgres from backup! New instance failed to become healthy!");
                    return null;
                }

                PostgresPersistedInstanceInfo persistedInstanceInfo = PostgresPersistedInstanceInfo
                        .builder()
                        .primary(true)
                        .instanceId(UUID.randomUUID())
                        .adapterIdentifier(primaryAdapterInstanceInfo.getAdapterInstanceId())
                        .build();

                raftFunctionalityCombinator.savePostgresNodeInfoInRaft(persistedInstanceInfo);

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

    private void checkPrimaryHealthAndFailoverIfNeeded() {
        List<PostgresCombinedInstanceInfo> availableInstances = orchestratorUtils.getCombinedInfosForAvailableInstances();

        PostgresCombinedInstanceInfo newPrimaryInstanceInfo = null;

        PostgresCombinedInstanceInfo currentPrimary = availableInstances.stream()
                .filter(info -> info.getPersisted().isPrimary())
                .findFirst()
                .orElse(null);

        if (currentPrimary == null || !currentPrimary.getAdapter().isActive()) {
            healthcheckFailedCount = Integer.MAX_VALUE;
            log.error("CAN NOT FIND POSTGRES PRIMARY. HEALTHCHECK FAILED!");
        } else {
            boolean healthy = postgresHealthcheckService.get().checkPostgresLiveliness(currentPrimary.getAdapter().getInstanceAddress(), currentPrimary.getAdapter().getInstancePort(), HEALTHCHECK_TIMEOUT);
            if (!currentPrimary.getAdapter().isActive() || !healthy) {
                healthcheckFailedCount++;
                primaryUnhealthy.set(true);
                log.error("POSTGRES PRIMARY UNHEALTHY. {} HEALTHCHECKS FAILED WHEN MAXIMUM IS {}", healthcheckFailedCount, orchestrationProperties.common().postgresDeadCheck().retries());
            } else {
                primaryUnhealthy.set(false);
            }
        }

        if (healthcheckFailedCount < orchestrationProperties.common().postgresDeadCheck().retries()) {
            return;
        }

        log.error("REACHED MAXIMUM HEALTHCHECKS RETRY COUNT. NEED TO FAILOVER.");

        // mark orchestrator as not ready, to prevent any other changes during failover
        orchestratorReady.set(false);

        newPrimaryInstanceInfo = selectNewPrimary(availableInstances);
        if (newPrimaryInstanceInfo == null) {
            log.error("FATAL ERROR. POSTGRES PRIMARY IS UNHEALTHY BUT NO ALIVE AND ACTIVE STANDBY FOUND. CAN NOT FAILOVER IMMEDIATELY!");

            if (!orchestrationProperties.postgresClusterRestore().autoRestoreLostCluster()) {
                log.error("WILL NOT TRY TO RESTORE CLUSTER FROM BACKUP, BECAUSE CONFIGURATION PROHIBITS THIS ACTION! RESTORE PRIMARY MANUALLY AND RESTART PGFACADE IN 'RECOVERY' MODE!");
                // keep orchestrator inactive.
                return;
            } else {
                UUID switchoverEventId = UUID.randomUUID();

                log.error("RESTORING PRIMARY USING LATEST AVAILABLE VERSION FROM ARCHIVE.");
                raftFunctionalityCombinator.notifyAllClusterAboutSwitchoverStarted(new SwitchoverStartedEvent(switchoverEventId));

                PostgresCombinedInstanceInfo combinedInstanceInfo = restoreClusterFromBackup(false);
                if (combinedInstanceInfo == null) {
                    raftFunctionalityCombinator.notifyAllClusterAboutSwitchoverCompleted(new SwitchoverCompletedEvent(switchoverEventId, false));
                    log.error("FAILED TO RESTORE PRIMARY FROM BACKUP!");
                    orchestratorReady.set(true);
                    return;
                }

                raftFunctionalityCombinator.notifyAllClusterAboutSwitchoverCompleted(new SwitchoverCompletedEvent(switchoverEventId, true));

                log.info("SUCCESSFULLY RESTORED LOST CLUSTER FROM BACKUP!");
                healthcheckFailedCount = 0;
                primaryUnhealthy.set(false);
            }
        } else {
            log.info("FAILOVER STARTED. STANDBY WILL SWITCHOVER PRIMARY.");

            boolean switchoverSucceeded = switchover(newPrimaryInstanceInfo, currentPrimary);
            if (!switchoverSucceeded) {
                log.error("FATAL ERROR. TRIED TO PROMOTE STANDBY BUT FAILED. FAILOVER FAILED!!!");
                orchestratorReady.set(true);
                return;
            } else {
                PostgresAdapterInstanceInfo adapterInstanceInfo = waitUntilPostgresInstanceHealthy(newPrimaryInstanceInfo.getAdapter().getAdapterInstanceId());
                if (adapterInstanceInfo == null) {
                    log.error("FAILED TO ACHIEVE HEALTH PRIMARY AFTER SWITCHOVER!");
                    orchestratorReady.set(true);
                    return;
                }
                healthcheckFailedCount = 0;
                primaryUnhealthy.set(false);
            }
        }

        orchestratorReady.set(true);
    }

    private PostgresCombinedInstanceInfo selectNewPrimary(List<PostgresCombinedInstanceInfo> availableInstances) {
        //TODO maybe add some better logic to select new primary. By LSN?
        return availableInstances
                .stream()
                .parallel()
                .filter(info ->
                        info.getAdapter().isActive()
                                && Boolean.FALSE.equals(info.getPersisted().isPrimary())
                                && postgresHealthcheckService.get().checkPostgresLiveliness(info.getAdapter().getInstanceAddress(), info.getAdapter().getInstancePort(), HEALTHCHECK_TIMEOUT)
                )
                .filter(info -> !restartingStandbyInstanceIds.contains(info.getPersisted().getInstanceId()))
                .findFirst()
                .orElse(null);
    }

    private void checkAndFixStandbyCount(List<PostgresCombinedInstanceInfo> availableInstances) {
        //remove unhealthy or inactive standby
        availableInstances
                .stream()
                .filter(info -> !restartingStandbyInstanceIds.contains(info.getPersisted().getInstanceId()))
                .filter(info -> Boolean.FALSE.equals(info.getPersisted().isPrimary()))
                .filter(info ->
                        !info.getAdapter().isActive()
                                || !postgresHealthcheckService.get().checkPostgresLiveliness(info.getAdapter().getInstanceAddress(), info.getAdapter().getInstancePort(), HEALTHCHECK_TIMEOUT)

                )
                .forEach(info -> {
                    if (raftFunctionalityCombinator.testIfAbleToCommitToRaftNoException()) {
                        log.info("Found unhealthy or inactive standby. Removing it.");
                        removeStandby(info);
                    }
                });

        newlyCreatedStartingStandbys.forEach((key, value) -> {
            if (value.isAfter(Instant.now().plus(15, ChronoUnit.SECONDS))) {
                newlyCreatedStartingStandbys.remove(key);
            }
        });

        List<PostgresCombinedInstanceInfo> healthyOrStartingStandby = availableInstances
                .stream()
                .filter(info -> Boolean.FALSE.equals(info.getPersisted().isPrimary()))
                .filter(info ->
                        newlyCreatedStartingStandbys.containsKey(info.getPersisted().getInstanceId())
                                || postgresHealthcheckService.get().checkPostgresLiveliness(info.getAdapter().getInstanceAddress(), info.getAdapter().getInstancePort(), HEALTHCHECK_TIMEOUT)
                )
                .toList();

        long countDiff = orchestrationProperties.common().standby().count() - healthyOrStartingStandby.size();

        if (healthyOrStartingStandby.size() < orchestrationProperties.common().standby().count() && raftFunctionalityCombinator.testIfAbleToCommitToRaftNoException()) {
            log.warn("Found {} starting or healthy standby while it is required to have {}. Need to start {} more.",
                    healthyOrStartingStandby.size(),
                    orchestrationProperties.common().standby().count(),
                    countDiff
            );

            List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();

            for (int i = 0; i < countDiff; i++) {
                completableFutureList.add(
                        managedExecutor.runAsync(
                                () -> {
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
                        )
                );
            }

            try {
                CompletableFuture.allOf(completableFutureList.toArray(new CompletableFuture[0])).join();
            } catch (Exception e) {
                log.error("Error while scaling up standby count", e);
            }
        } else if (healthyOrStartingStandby.size() > orchestrationProperties.common().standby().count()) {
            log.warn("Found {} starting or healthy standby while it is required to have {}. Will stop {} instance(s).",
                    healthyOrStartingStandby.size(),
                    orchestrationProperties.common().standby().count(),
                    Math.abs(countDiff)
            );

            for (int i = 0; i < Math.abs(countDiff); i++) {
                platformAdapter.get().deleteInstance(healthyOrStartingStandby.get(i).getAdapter().getAdapterInstanceId());
            }
        }
    }

    private void removeStandby(PostgresCombinedInstanceInfo standbyInstanceInfo) {
        try {
            raftFunctionalityCombinator.deletePostgresNodeInfoInRaft(standbyInstanceInfo.getPersisted().getInstanceId());
            platformAdapter.get().deleteInstance(standbyInstanceInfo.getAdapter().getAdapterInstanceId());
            postgresUtils.dropPhysicalReplicationSlotOnPrimarySafely(standbyInstanceInfo.getPersisted().getReplicationSlotName());
        } catch (RaftException e) {
            log.error("Failed to delete instance {} in raft!", standbyInstanceInfo.getPersisted().getInstanceId(), e);
        }
    }

    private synchronized boolean switchover(PostgresCombinedInstanceInfo newPrimaryInstanceInfo, PostgresCombinedInstanceInfo currentPrimaryInstanceInfo) {
        UUID switchoverEventId = UUID.randomUUID();

        if (!raftFunctionalityCombinator.testIfAbleToCommitToRaftNoException()) {
            return false;
        }

        switchoverInProgress.set(true);

        try {
            raftFunctionalityCombinator.notifyAllClusterAboutSwitchoverStarted(new SwitchoverStartedEvent(switchoverEventId));

            log.info("SWITCHOVER STARTED. SWITCHING TO INSTANCE WITH IP {} AND PORT {}", newPrimaryInstanceInfo.getAdapter().getInstanceAddress(), newPrimaryInstanceInfo.getAdapter().getInstancePort());

            Connection standbyConnection = runtimePostgresConnectionProducer.createNewPgFacadeUserConnection(
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

            newPrimaryInstanceInfo.getPersisted().setPrimary(true);
            raftFunctionalityCombinator.updatePostgresNodeInfoInRaft(newPrimaryInstanceInfo.getPersisted());

            if (currentPrimaryInstanceInfo != null) {
                clusterRuntimeProperties.getAllPostgresInstancesInfos().remove(currentPrimaryInstanceInfo.getPersisted().getInstanceId());
                platformAdapter.get().deleteInstance(currentPrimaryInstanceInfo.getAdapter().getAdapterInstanceId());
                raftFunctionalityCombinator.deletePostgresNodeInfoInRaft(currentPrimaryInstanceInfo.getPersisted().getInstanceId());
            }

            //remove all standby because of new timeline
            //TODO it is possible to repair such nodes instead of deleting them. Use recovery_target_timeline = 'latest'
            log.info("NEW PRIMARY PROMOTED. REMOVING ALL FORMER STANDBY BECAUSE OF NEW TIMELINE.");
            orchestratorUtils.getCombinedInfosForStandbyInstances().forEach(this::removeStandby);

            raftFunctionalityCombinator.notifyAllClusterAboutSwitchoverCompleted(new SwitchoverCompletedEvent(switchoverEventId, true));
            log.info("SWITCHOVER COMPLETED SUCCESSFULLY");
            standbyConnection.close();
            return true;
        } catch (Exception e) {
            try {
                raftFunctionalityCombinator.notifyAllClusterAboutSwitchoverCompleted(new SwitchoverCompletedEvent(switchoverEventId, false));
            } catch (RaftException ex) {
                log.error("Failed to notify all PgFacade nodes about failed switchover!", ex);
            }
            log.error("ERROR DURING SWITCHOVER", e);
            return false;
        } finally {
            switchoverInProgress.set(false);
        }
    }

    private boolean restartStandbyAndWaitUntilItIsReadyAndRemoveOnFail(PostgresCombinedInstanceInfo standbyInfo) {
        UUID instanceId = standbyInfo.getPersisted().getInstanceId();
        restartingStandbyInstanceIds.add(instanceId);
        orchestratorUtils.removeInstanceFromRuntimePropertiesAndNotifyAllIfStandby(instanceId);

        try {
            raftFunctionalityCombinator.deletePostgresNodeInfoInRaft(instanceId);
            platformAdapter.get().restartPostgresInstance(standbyInfo.getAdapter().getAdapterInstanceId());

            PostgresAdapterInstanceInfo adapterInstanceInfo = waitUntilPostgresInstanceHealthy(standbyInfo.getAdapter().getAdapterInstanceId());
            if (adapterInstanceInfo == null) {
                log.error("Standby failed to become healthy after restart! Removing it...");
                restartingStandbyInstanceIds.remove(instanceId);
                removeStandby(standbyInfo);
                return false;
            }

            raftFunctionalityCombinator.savePostgresNodeInfoInRaft(standbyInfo.getPersisted());
            return true;
        } catch (Exception e) {
            log.error("Error while restarting standby! It will be removed!", e);
            restartingStandbyInstanceIds.remove(instanceId);
            removeStandby(standbyInfo);
            return false;
        }
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

        PostgresAdapterInstanceInfo postgresAdapterInstanceInfo = startPostgresInstanceAndWaitToBeReady(adapterIdentifier);
        if (postgresAdapterInstanceInfo == null) {
            log.error("Newly created standby failed to start!");
            platformAdapter.get().deleteInstance(adapterIdentifier);
            postgresUtils.dropPhysicalReplicationSlotOnPrimarySafely(physicalSlotName);
            return null;
        }

        boolean settingsChanged = configureStandbyForReplication(
                postgresAdapterInstanceInfo.getAdapterInstanceId(),
                postgresAdapterInstanceInfo.getInstanceAddress(),
                postgresAdapterInstanceInfo.getInstancePort(),
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

    private boolean configureStandbyForReplication(String adapterIdentifier, String address, int port, String standbyName, String replicationSlotName) {
        log.info("Standby stared and is healthy. Changing it's settings to achieve replication.");

        try (Connection connection = runtimePostgresConnectionProducer.createNewPgFacadeUserConnection(address, port)) {
            Map<String, String> standbySettings = new LinkedHashMap<>();
            standbySettings.put(
                    PostgresConstants.CLUSTER_NAME_SETTING_NAME,
                    standbyName
            );
            standbySettings.put(
                    PostgresConstants.PRIMARY_SLOT_NAME_SETTING_NAME,
                    replicationSlotName
            );
            standbySettings.put(
                    PostgresConstants.PRIMARY_CONN_INFO_SETTING_NAME,
                    postgresUtils.getPrimaryConnInfoSetting()
            );

            boolean settingsChanged = postgresConfigurator.changePostgresSettingsFastUnsafe(
                    standbySettings,
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

        // restart due to cluster_name settings change
        try {
            platformAdapter.get().restartPostgresInstance(adapterIdentifier);
        } catch (Exception e) {
            log.error("Failed to restart standby after configuring it for replication!", e);
        }

        PostgresAdapterInstanceInfo adapterInstanceInfo = waitUntilPostgresInstanceHealthy(adapterIdentifier);
        if (adapterInstanceInfo == null) {
            log.error("Failed to restart standby after configuring it for replication!");
            return false;
        }

        return true;
    }

    private PostgresAdapterInstanceInfo startPostgresInstanceAndWaitToBeReady(String adapterIdentifier) {
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

    private PostgresAdapterInstanceInfo waitUntilPostgresInstanceHealthy(String adapterInstanceId) {
        OrchestrationProperties.CommonProperties.PostgresStartupCheckProperties startupCheckProperties = orchestrationProperties.common().postgresStartupCheck();

        long endTime = System.currentTimeMillis() + (startupCheckProperties.interval() * startupCheckProperties.retries()) + startupCheckProperties.startPeriod();
        PostgresAdapterInstanceInfo instanceInfo = platformAdapter.get().getPostgresInstanceInfo(adapterInstanceId);

        try {
            boolean succeeded = false;
            while (endTime > System.currentTimeMillis()) {
                boolean healthcheckSucceeded = postgresHealthcheckService.get().checkPostgresLiveliness(instanceInfo.getInstanceAddress(), instanceInfo.getInstancePort(), HEALTHCHECK_TIMEOUT);
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

    private void waitForActiveOperationsToComplete() {
        while (livelinessCheckInProgress.get() || switchoverInProgress.get() || standbyCountCheckInProgress.get()) {
            // waiting for all operations to complete.
        }
    }
}
