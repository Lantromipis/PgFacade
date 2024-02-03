package com.lantromipis.orchestration.orchestrator.impl;

import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.properties.predefined.ArchivingProperties;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.configuration.properties.runtime.PostgresSettingsRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.OrchestratorNotReadyException;
import com.lantromipis.orchestration.exception.OrchestratorOperationExecutionException;
import com.lantromipis.orchestration.exception.RaftException;
import com.lantromipis.orchestration.model.*;
import com.lantromipis.orchestration.model.raft.PostgresPersistedInstanceInfo;
import com.lantromipis.orchestration.model.raft.PostgresSwitchoverCompletedNotification;
import com.lantromipis.orchestration.model.raft.PostgresSwitchoverStartedNotification;
import com.lantromipis.orchestration.orchestrator.api.PostgresOrchestrator;
import com.lantromipis.orchestration.service.api.*;
import com.lantromipis.orchestration.util.JdbcUtils;
import com.lantromipis.orchestration.util.OrchestratorUtils;
import com.lantromipis.orchestration.util.PostgresUtils;
import com.lantromipis.orchestration.util.RaftFunctionalityCombinator;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.eclipse.microprofile.context.ManagedExecutor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
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
    PostgresConfigurationService postgresConfigurationService;

    @Inject
    PostgresUtils postgresUtils;

    @Inject
    ArchivingProperties archivingProperties;

    @Inject
    PostgresArchivingService postgresArchivingService;

    @Inject
    OrchestratorUtils orchestratorUtils;

    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    @Inject
    RaftFunctionalityCombinator raftFunctionalityCombinator;

    @Inject
    Instance<PostgresHealthcheckService> postgresHealthcheckService;

    @Inject
    ManagedExecutor managedExecutor;

    @Inject
    PostgresSettingsRuntimeProperties postgresSettingsRuntimeProperties;

    @Inject
    PostgresPrimaryOrchestrationService postgresPrimaryOrchestrationService;

    @Inject
    PostgresStandbyOrchestrationService postgresStandbyOrchestrationService;

    private final AtomicBoolean orchestratorReady = new AtomicBoolean(false);
    private final AtomicBoolean switchoverInProgress = new AtomicBoolean(false);
    private final AtomicBoolean primaryUnhealthy = new AtomicBoolean(false);
    private int healthcheckFailedCount = 0;
    private final Set<UUID> restartingStandbyInstanceIds = ConcurrentHashMap.newKeySet();

    private final static long HEALTHCHECK_TIMEOUT = 1500;

    private final Object switchoverLock = new Object[0];

    @Override
    public void initializeWhenClusterRunning() throws Exception {
        if (PgFacadeRaftRole.FOLLOWER.equals(pgFacadeRuntimeProperties.getRaftRole())) {
            log.info("Not starting Postgres orchestration because this PgFacade instance is not current raft leader.");
            postgresSettingsRuntimeProperties.reload();

            if (archivingProperties.enabled()) {
                postgresArchivingService.initialize();
            }
            log.info("Fast orchestrator initialization completed!");
            return;
        }

        if (archivingProperties.enabled()) {
            postgresArchivingService.startArchiving();
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
    public void initializeWhenClusterStopped() throws Exception {
        if (PgFacadeRaftRole.FOLLOWER.equals(pgFacadeRuntimeProperties.getRaftRole())) {
            log.info("Not starting Postgres orchestration because this PgFacade instance is not current raft leader.");
            postgresSettingsRuntimeProperties.reload();
            return;
        }

        // start primary
        PostgresCombinedInstanceInfo primaryInstanceInfo = postgresPrimaryOrchestrationService.startStoppedExistingPrimary();

        orchestratorUtils.addInstanceToRuntimePropertiesAndNotifyAllIfStandby(primaryInstanceInfo);
        postgresSettingsRuntimeProperties.reload();

        log.info("Primary is up and running!");

        // start standbys
        List<PostgresCombinedInstanceInfo> startedStandbyInstanceInfos = postgresStandbyOrchestrationService.startStoppedStandbys();
        startedStandbyInstanceInfos.forEach(
                standbyInfo -> orchestratorUtils.addInstanceToRuntimePropertiesAndNotifyAllIfStandby(standbyInfo)
        );

        // archiving
        if (archivingProperties.enabled()) {
            postgresArchivingService.initialize();
            postgresArchivingService.startArchiving();
        } else {
            log.warn("Archiving is disabled. Continuous Archiving and Point-in-Time Recovery will not be possible!");
        }

        log.info("Orchestrator initialization completed!");

        orchestratorReady.set(true);
    }

    @Override
    public void stopOrchestrator(boolean shutdownPostgres) {
        orchestratorReady.set(false);
        postgresArchivingService.stop();
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

        PostgresSettingsValidationResult validationResult = postgresConfigurationService.validateSettingAndCheckIfRestartRequired(newSettingNamesAndValuesMap);
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
                PostgresInstanceSettingsChangeResult changeResult = postgresConfigurationService.changePostgresInstanceSettingsAndRollbackOnFailure(
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

        // in case restart required, restart primary first, and only then standby
        // this will guarantee that wrong settings won't kill cluster
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

        UUID switchoverNotificationId = UUID.randomUUID();

        try {
            log.warn("RESTARTING CLUSTER DUE TO POSTGRES SETTINGS CHANGES. CLUSTER WILL BE TEMPORARY UNAVAILABLE.");

            // using switchover event because for application restart looks the same
            PostgresSwitchoverStartedNotification switchoverStartedNotification = PostgresSwitchoverStartedNotification
                    .builder()
                    .notificationId(switchoverNotificationId)
                    .build();
            raftFunctionalityCombinator.notifyAllClusterAboutSwitchoverStarted(switchoverStartedNotification);

            PostgresInstanceSettingsChangeResult primaryChangeResult = postgresConfigurationService.changePostgresInstanceSettingsAndRollbackOnFailure(
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

            PostgresAdapterInstanceInfo adapterInstanceInfo = orchestratorUtils.restartPostgresInstanceAndWaitToBeReady(primaryCombinedInstanceInfo.getAdapter().getAdapterInstanceId());
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
                PostgresInstanceSettingsChangeResult standbyChangeResult = postgresConfigurationService.changePostgresInstanceSettingsAndRollbackOnFailure(
                        availableStandby.getPersisted().getInstanceId(),
                        newSettingNamesAndValuesMap
                );

                // failed to rollback
                if (!PostgresInstanceSettingsChangeResult.Status.SUCCESS.equals(standbyChangeResult.getStatus())
                        && standbyChangeResult.isRollbackWasRequired()
                        && CollectionUtils.isNotEmpty(standbyChangeResult.getNotRollbackedSettings())) {
                    log.error("FAILED TO ROLLBACK INCORRECT SETTINGS FOR STANDBY!");
                    removeStandby(availableStandby.getPersisted());
                    continue;
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
                PostgresSwitchoverCompletedNotification switchoverCompletedNotification =
                        PostgresSwitchoverCompletedNotification
                                .builder()
                                .success(false)
                                .build();
                raftFunctionalityCombinator.notifyAllClusterAboutSwitchoverCompleted(switchoverCompletedNotification);
            } catch (Exception e) {
                log.error("Failed to notify cluster about switchover completed!", e);
            }
        }
    }

    @Blocking
    @Scheduled(every = "${pg-facade.orchestration.common.postgres.primary.healthcheck.interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void checkPrimaryLiveliness() {
        if (orchestratorReady.get() && !switchoverInProgress.get()) {
            checkPrimaryHealthAndFailoverIfNeeded();
        }
    }

    @Blocking
    @Scheduled(every = "${pg-facade.orchestration.common.postgres.standby.healthcheck.interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void checkStandbyCount() {
        if (orchestratorReady.get() && !primaryUnhealthy.get() && !switchoverInProgress.get()) {
            postgresStandbyOrchestrationService.checkStandbyCountAndLiveliness();
        }
    }

    private void checkPrimaryHealthAndFailoverIfNeeded() {
        List<PostgresCombinedInstanceInfo> availableInstances = orchestratorUtils.getCombinedInfosForAvailableInstances();

        PostgresCombinedInstanceInfo currentPrimary = availableInstances.stream()
                .filter(info -> info.getPersisted().isPrimary())
                .findFirst()
                .orElse(null);

        var primaryHealthcheckProperties = orchestrationProperties.common().postgres().primary().healthcheck();

        if (currentPrimary == null || !currentPrimary.getAdapter().isActive()) {
            healthcheckFailedCount = Integer.MAX_VALUE;
            log.error("CAN NOT FIND POSTGRES PRIMARY. HEALTHCHECK FAILED!");
        } else {
            boolean healthy = postgresHealthcheckService.get().checkPostgresLiveliness(
                    currentPrimary.getAdapter().getInstanceAddress(),
                    currentPrimary.getAdapter().getInstancePort(),
                    primaryHealthcheckProperties.timeout().toMillis()
            );
            if (!currentPrimary.getAdapter().isActive() || !healthy) {
                healthcheckFailedCount++;
                primaryUnhealthy.set(true);
                log.error("POSTGRES PRIMARY UNHEALTHY. {} HEALTHCHECKS FAILED WHEN MAXIMUM IS {}",
                        healthcheckFailedCount,
                        primaryHealthcheckProperties.retries()
                );
            } else {
                primaryUnhealthy.set(false);
            }
        }

        if (healthcheckFailedCount < primaryHealthcheckProperties.retries()) {
            return;
        }

        log.error("REACHED MAXIMUM HEALTHCHECKS RETRY COUNT. NEED TO FAILOVER.");

        // mark orchestrator as not ready, to prevent any other changes during failover
        orchestratorReady.set(false);

        StandbyElectionForPromotionResult standbyElectionForPromotionResult = postgresStandbyOrchestrationService.selectStandbyForPromotion();
        if (standbyElectionForPromotionResult == null) {
            log.error("FATAL ERROR. POSTGRES PRIMARY IS UNHEALTHY BUT NO ALIVE AND ACTIVE STANDBY FOUND. CAN NOT FAILOVER IMMEDIATELY!");
            orchestratorReady.set(true);
            return;
        }
        log.info("FAILOVER STARTED. STANDBY WILL SWITCHOVER PRIMARY.");

        boolean switchoverSucceeded = switchover(standbyElectionForPromotionResult, currentPrimary);
        standbyElectionForPromotionResult.freeResources();
        if (!switchoverSucceeded) {
            log.error("FATAL ERROR. TRIED TO PROMOTE STANDBY BUT FAILED. FAILOVER FAILED!!!");
            orchestratorReady.set(true);
            return;
        }

        PostgresAdapterInstanceInfo adapterInstanceInfo = orchestratorUtils.waitUntilPostgresInstanceHealthy(
                standbyElectionForPromotionResult.getElectedForPromotionStandby().getAdapter().getAdapterInstanceId()
        );
        if (adapterInstanceInfo == null) {
            log.error("FAILED TO ACHIEVE HEALTH PRIMARY AFTER SWITCHOVER!");
            orchestratorReady.set(true);
            return;
        }
        healthcheckFailedCount = 0;
        primaryUnhealthy.set(false);
        orchestratorReady.set(true);
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

    @Synchronized("switchoverLock")
    private boolean switchover(StandbyElectionForPromotionResult standbyElectionForPromotionResult, PostgresCombinedInstanceInfo currentPrimaryInstanceInfo) {
        UUID switchoverNotificationId = UUID.randomUUID();

        switchoverInProgress.set(true);

        try {
            PostgresSwitchoverStartedNotification switchoverStartedNotification = PostgresSwitchoverStartedNotification
                    .builder()
                    .notificationId(switchoverNotificationId)
                    .build();
            raftFunctionalityCombinator.notifyAllClusterAboutSwitchoverStarted(switchoverStartedNotification);
        } catch (Exception e) {
            log.error("Failed to notify PgFacade cluster about switchover start! Is this node a leader?", e);
            switchoverInProgress.set(false);
            return false;
        }

        // not using try-catch-with resources to guarantee that exception on close() wont affect method return result
        Connection standbyConnection = standbyElectionForPromotionResult.getElectedForPromotionStandbyConnection();
        Statement promoteStatement = null;

        try {
            PostgresCombinedInstanceInfo newPrimaryInstanceInfo = standbyElectionForPromotionResult.getElectedForPromotionStandby();
            log.info("SWITCHOVER STARTED. SWITCHING TO INSTANCE WITH IP {} AND PORT {}",
                    newPrimaryInstanceInfo.getAdapter().getInstanceAddress(),
                    newPrimaryInstanceInfo.getAdapter().getInstancePort()
            );

            promoteStatement = standbyConnection.createStatement();
            ResultSet promoteResultSet = promoteStatement.executeQuery("SELECT pg_promote()");
            promoteResultSet.next();
            boolean promoteSuccessful = promoteResultSet.getBoolean(1);
            if (!promoteSuccessful) {
                // TODO check if this server is already a primary
                log.error("ERROR DURING SWITCHOVER. STANDBY CANT COMPLETE PROMOTE REQUEST.");
                return false;
            }

            // will remove all standby because of new timeline
            // TODO it is possible to repair such nodes instead of deleting them. Use pg_rewind
            log.info("NEW PRIMARY PROMOTED. REMOVING ALL FORMER STANDBY BECAUSE OF NEW TIMELINE.");
            newPrimaryInstanceInfo.getPersisted().setPrimary(true);
            Set<UUID> instancesToRemove = new HashSet<>();

            if (MapUtils.isNotEmpty(standbyElectionForPromotionResult.getOtherStandbys())) {
                for (var entry : standbyElectionForPromotionResult.getOtherStandbys().entrySet()) {
                    instancesToRemove.add(entry.getKey());
                    // no need to remove replication slot on primary!
                    platformAdapter.get().deleteInstance(entry.getValue().getAdapter().getAdapterInstanceId());
                }
            }

            if (currentPrimaryInstanceInfo != null) {
                instancesToRemove.add(currentPrimaryInstanceInfo.getPersisted().getInstanceId());
                managedExecutor.runAsync(() -> platformAdapter.get().deleteInstance(currentPrimaryInstanceInfo.getAdapter().getAdapterInstanceId()));
            }

            PostgresSwitchoverCompletedNotification switchoverCompletedNotification =
                    PostgresSwitchoverCompletedNotification
                            .builder()
                            .success(true)
                            .notificationId(switchoverNotificationId)
                            .instanceToRemoveIds(instancesToRemove)
                            .newPrimaryCombinedInfo(standbyElectionForPromotionResult.getElectedForPromotionStandby())
                            .build();

            raftFunctionalityCombinator.notifyAllClusterAboutSwitchoverCompleted(switchoverCompletedNotification);
            log.info("SWITCHOVER COMPLETED SUCCESSFULLY");
            return true;
        } catch (Exception e) {
            try {
                PostgresSwitchoverCompletedNotification switchoverCompletedNotification =
                        PostgresSwitchoverCompletedNotification
                                .builder()
                                .success(false)
                                .build();
                raftFunctionalityCombinator.notifyAllClusterAboutSwitchoverCompleted(switchoverCompletedNotification);
            } catch (RaftException ex) {
                log.error("Failed to notify all PgFacade nodes about failed switchover!", ex);
            }
            log.error("ERROR DURING SWITCHOVER", e);
            return false;
        } finally {
            switchoverInProgress.set(false);
            JdbcUtils.closeJdbcStatementSafely(promoteStatement);
            JdbcUtils.closeJdbcConnectionSafely(standbyConnection);
        }
    }

    private boolean restartStandbyAndWaitUntilItIsReadyAndRemoveOnFail(PostgresCombinedInstanceInfo standbyInfo) {
        UUID instanceId = standbyInfo.getPersisted().getInstanceId();
        restartingStandbyInstanceIds.add(instanceId);
        orchestratorUtils.removeInstanceFromRuntimePropertiesAndNotifyAllIfStandby(instanceId);

        try {
            raftFunctionalityCombinator.deletePostgresNodeInfoInRaft(instanceId);
            platformAdapter.get().restartPostgresInstance(standbyInfo.getAdapter().getAdapterInstanceId());

            PostgresAdapterInstanceInfo adapterInstanceInfo = orchestratorUtils.waitUntilPostgresInstanceHealthy(standbyInfo.getAdapter().getAdapterInstanceId());
            if (adapterInstanceInfo == null) {
                log.error("Standby failed to become healthy after restart! Removing it...");
                restartingStandbyInstanceIds.remove(instanceId);
                removeStandby(standbyInfo.getPersisted());
                return false;
            }

            raftFunctionalityCombinator.savePostgresNodeInfoInRaft(standbyInfo.getPersisted());
            return true;
        } catch (Exception e) {
            log.error("Error while restarting standby! It will be removed!", e);
            restartingStandbyInstanceIds.remove(instanceId);
            removeStandby(standbyInfo.getPersisted());
            return false;
        }
    }

    private void waitForActiveOperationsToComplete() {
        while (switchoverInProgress.get()) {
            // waiting for all operations to complete.
        }
    }
}
