package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.event.*;
import com.lantromipis.configuration.model.PostgresPersistedSettingInfo;
import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import com.lantromipis.configuration.properties.predefined.ArchivingProperties;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.configuration.properties.stored.api.PostgresPersistedProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.*;
import com.lantromipis.orchestration.model.*;
import com.lantromipis.orchestration.service.api.PostgresArchiver;
import com.lantromipis.orchestration.service.api.PostgresConfigurator;
import com.lantromipis.orchestration.service.api.PostgresOrchestrator;
import com.lantromipis.orchestration.util.PostgresUtils;
import io.quarkus.scheduler.Scheduled;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
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
import java.util.stream.Collectors;

@Slf4j
@ApplicationScoped
public class PostgresOrchestratorImpl implements PostgresOrchestrator {
    @Inject
    Instance<PlatformAdapter> orchestrationAdapter;

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    OrchestrationProperties orchestrationProperties;

    @Inject
    Event<PrimaryReadyEvent> masterReadyEvent;

    @Inject
    Event<SwitchoverCompletedEvent> switchoverCompletedEvent;

    @Inject
    Event<SwitchoverStartedEvent> switchoverStartedEvent;

    @Inject
    Event<StandbyRemoveStartedEvent> standbyRemoveStartedEvent;

    @Inject
    Event<StandbyRestartStartedEvent> standbyRestartStartedEvent;

    @Inject
    Event<StandbyRestartCompletedEvent> standbyRestartCompletedEvent;

    @Inject
    Event<StandbyRemoveCompletedEvent> standbyRemoveCompletedEvent;

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

    private final AtomicBoolean orchestratorReady = new AtomicBoolean(false);
    private final AtomicBoolean livelinessCheckInProgress = new AtomicBoolean(false);
    private final AtomicBoolean standbyCountCheckInProgress = new AtomicBoolean(false);
    private final AtomicBoolean switchoverInProgress = new AtomicBoolean(false);
    private final AtomicBoolean clusterRestartInProgress = new AtomicBoolean(false);
    private int healthcheckFailedCount = 0;
    private Set<UUID> restartingStandbyInstanceIds = ConcurrentHashMap.newKeySet();

    private UUID masterInstanceId;

    @Override
    public void initialize() {
        if (OrchestrationProperties.AdapterType.NO_ADAPTER.equals(orchestrationProperties.adapter())) {
            log.info("No orchestrator adapter is configured. PgFacade will work like proxy + connection pool without any HA features for Postgres. Consider choosing another adapter if you need HA features.");
            addInstanceToRuntimeProperties(PostgresAdapterInstanceInfo
                    .builder()
                    .master(true)
                    .instanceId(UUID.randomUUID())
                    .instancePort(orchestrationProperties.noAdapter().primaryPort())
                    .instanceAddress(orchestrationProperties.noAdapter().primaryHost())
                    .build()
            );

            // to set runtime properties
            postgresConfigurator.initialize();

            return;
        }

        orchestrationAdapter.get().initialize();

        // primary section
        PostgresAdapterInstanceInfo primaryInstanceInfo = orchestrationAdapter.get().getAvailablePostgresInstancesInfos().stream().filter(info -> Boolean.TRUE.equals(info.getMaster())).findFirst().orElse(null);

        boolean isNewDBSetup = false;

        if (primaryInstanceInfo == null) {
            log.info("Can not find any Postgres primary instance. Will create and start new one.");
            primaryInstanceInfo = createStartAndWaitForNewInstanceToBeReady(true);
            isNewDBSetup = true;
        } else if (InstanceStatus.NOT_ACTIVE.equals(primaryInstanceInfo.getStatus())) {
            log.info("Found non-active Postgres primary instance. Will start it now.");
            boolean masterStarted = orchestrationAdapter.get().startPostgresInstance(primaryInstanceInfo.getInstanceId());

            if (!masterStarted) {
                log.info("Can not start non-active primary instance. Will delete it and create and start new one.");
                orchestrationAdapter.get().deletePostgresInstance(primaryInstanceInfo.getInstanceId(), true);
                primaryInstanceInfo = createStartAndWaitForNewInstanceToBeReady(true);
                isNewDBSetup = true;
            } else {
                primaryInstanceInfo = waitUntilPostgresInstanceHealthy(primaryInstanceInfo.getInstanceId());
            }
        } else if (InstanceStatus.ACTIVE.equals(primaryInstanceInfo.getStatus())) {
            log.info("Found active Postgres primary. No actions needed.");
        }

        addInstanceToRuntimeProperties(primaryInstanceInfo);

        log.info("Primary is up and running!");

        if (isNewDBSetup) {
            postgresConfigurator.configureNewlyCreatedMaster();
        }

        masterInstanceId = primaryInstanceInfo.getInstanceId();
        masterReadyEvent.fire(new PrimaryReadyEvent());

        // standby section
        List<PostgresAdapterInstanceInfo> standbyInfos = orchestrationAdapter.get().getAvailablePostgresInstancesInfos()
                .stream()
                .filter(info -> Boolean.FALSE.equals(info.getMaster()))
                .toList();

        log.info("Checking standby count.");

        if (CollectionUtils.isNotEmpty(standbyInfos) && standbyInfos.stream().allMatch(info -> InstanceStatus.NOT_ACTIVE.equals(info.getStatus()))) {
            log.info("All standby inactive. Will start required amount.");
            // All standby inactive which means that PgFacade is starting up after full shutdown. Start required count.
            for (int i = 0; i < Math.min(standbyInfos.size(), orchestrationProperties.common().standby().count()); i++) {
                PostgresAdapterInstanceInfo standbyInfo = standbyInfos.get(i);
                boolean standbyStarted = orchestrationAdapter.get().startPostgresInstance(standbyInfo.getInstanceId());
                if (standbyStarted) {
                    waitUntilPostgresInstanceHealthy(standbyInfo.getInstanceId());
                }
            }
        }

        orchestrationAdapter.get().getAvailablePostgresInstancesInfos()
                .stream()
                .filter(info -> InstanceHealth.HEALTHY.equals(info.getHealth()))
                .forEach(this::addInstanceToRuntimeProperties);

        postgresConfigurator.initialize();

        validateDefaultSettingsPresence();

        if (archivingProperties.enabled()) {
            postgresArchiver.initialize();
        } else {
            log.warn("Arching is disabled. Continuous Archiving and Point-in-Time Recovery will not be possible!");
        }

        log.info("Orchestrator initialization completed!");

        orchestratorReady.set(true);
    }

    @Override
    public void shutdown() {
        orchestratorReady.set(false);
        while (livelinessCheckInProgress.get() || switchoverInProgress.get() || standbyCountCheckInProgress.get()) {
            //waiting for all operations to complete.
        }
        orchestrationAdapter.get().getAvailablePostgresInstancesInfos().forEach(info ->
                orchestrationAdapter.get().stopPostgresInstance(info.getInstanceId())
        );
        orchestrationAdapter.get().shutdown();
    }

    @Override
    public boolean switchover(UUID newMasterInstanceId) throws OrchestratorNotReadyException {
        if (orchestratorReady.get()) {
            return switchover(orchestrationAdapter.get().getInstanceInfo(newMasterInstanceId));
        } else {
            throw new OrchestratorNotReadyException("Can not switchover. Orchestrator not ready. Its initialization is still in progress or PgFacade is configured to work just like proxy.");
        }
    }

    @Override
    public void changePostgresSettings(Map<String, String> newSettingNamesAndValuesMap) throws PostgresConfigurationChangeException, OrchestratorNotReadyException {
        if (!orchestratorReady.get()) {
            throw new OrchestratorNotReadyException("Can not update Postgres settings. Orchestrator not ready. Its initialization is still in progress or PgFacade is configured to work just like proxy.");
        }

        if (switchoverInProgress.get()) {
            throw new PostgresConfigurationChangeException("Switchover in progress. For safety reasons, it is impossible to change settings now. Try again later when switchover completed.");
        }

        if (orchestratorReady.get() && !switchoverInProgress.get() && clusterRestartInProgress.compareAndSet(false, true)) {
            try {
                boolean restartRequired = postgresConfigurator.validateSettingAndCheckIfRestartRequired(newSettingNamesAndValuesMap);

                if (!restartRequired) {
                    orchestrationAdapter.get().getAvailablePostgresInstancesInfos()
                            .forEach(
                                    instanceInfo -> postgresConfigurator.changePostgresSettings(instanceInfo.getInstanceId(), newSettingNamesAndValuesMap)
                            );
                } else {
                    List<PostgresAdapterInstanceInfo> availableStandbys = orchestrationAdapter.get().getAvailablePostgresInstancesInfos()
                            .stream()
                            .filter(info ->
                                    Boolean.FALSE.equals(info.getMaster())
                                            && InstanceHealth.HEALTHY.equals(info.getHealth())
                                            && InstanceStatus.ACTIVE.equals(info.getStatus())
                            )
                            .toList();

                    if (CollectionUtils.isEmpty(availableStandbys)) {
                        throw new PostgresConfigurationChangeException("Provided settings require restart but currently there are no healthy standby nodes. For safety reasons, parameters can not be changed now. Try again later, when there will be at least one standby. However, it is highly recommended to change provided settings with at least 2 active standby nodes.");
                    }

                    if (availableStandbys.size() == 1) {
                        log.warn("Settings require restart but there is only 1 healthy standby. Potentially unsafe operation.");
                    }

                    log.warn("RESTARTING CLUSTER DUE TO POSTGRES SETTINGS CHANGES. CLUSTER WILL BE TEMPORARY UNAVAILABLE.");

                    // using switchover event because for application restart looks the same
                    UUID switchoverEventId = UUID.randomUUID();
                    switchoverStartedEvent.fire(new SwitchoverStartedEvent(switchoverEventId));

                    try {
                        postgresConfigurator.changePostgresSettings(masterInstanceId, newSettingNamesAndValuesMap);
                        orchestrationAdapter.get().restartPostgresInstance(masterInstanceId);

                        waitUntilPostgresInstanceHealthy(masterInstanceId);

                        // most likely we faced config parameter issue, so primary can not start.
                        // Because of that, there is no ability to revert settings (in non-running container), so instance must be deleted
                        orchestrationAdapter.get().deletePostgresInstance(masterInstanceId, true);
                    } catch (Throwable t) {
                        log.error("CLUSTER FAILED TO RESTART. WILL TRY TO RECOVER.");
                        switchoverCompletedEvent.fire(new SwitchoverCompletedEvent(switchoverEventId, false));
                        throw t;
                    }

                    log.warn("PRIMARY RESTARTED SUCCESSFULLY AND NEW POSTGRES SETTINGS WERE APPLIED. PRIMARY IS AVAILABLE NOW.");
                    switchoverCompletedEvent.fire(new SwitchoverCompletedEvent(switchoverEventId, true));

                    // all good for primary, so it will be good for everyone
                    for (PostgresAdapterInstanceInfo availableStandby : availableStandbys) {
                        UUID uuid = availableStandby.getInstanceId();
                        restartingStandbyInstanceIds.add(uuid);
                        postgresConfigurator.changePostgresSettings(uuid, newSettingNamesAndValuesMap);
                        restartStandbyAndWaitUntilItIsReady(uuid);
                    }
                    log.warn("CLUSTER RESTARTED SUCCESSFULLY AND NEW POSTGRES SETTINGS WERE APPLIED. CLUSTER IS AVAILABLE NOW.");
                }

                postgresPersistedProperties.savePostgresSettingsInfos(newSettingNamesAndValuesMap);

            } catch (PostgresConfigurationChangeException e) {
                throw e;
            } catch (Exception e) {
                throw new PostgresConfigurationChangeException("Unexpected error while updating Postgres settings.", e);
            } finally {
                clusterRestartInProgress.set(false);
            }
        } else if (clusterRestartInProgress.get()) {
            throw new PostgresConfigurationChangeException("Can not update Postgres settings. Currently restarting cluster due to settings changes.");
        } else {
            throw new PostgresConfigurationChangeException("Unexpected error while updating Postgres settings.");
        }
    }

    @Scheduled(every = "${pg-facade.orchestration.common.postgres-dead-check.interval}")
    public void checkPrimaryLiveliness() {
        if (orchestratorReady.get() && !switchoverInProgress.get() && !clusterRestartInProgress.get() && livelinessCheckInProgress.compareAndSet(false, true)) {
            List<PostgresAdapterInstanceInfo> availableInstances = orchestrationAdapter.get().getAvailablePostgresInstancesInfos();
            checkPrimaryHealthAndFailoverIfNeeded(availableInstances);
            livelinessCheckInProgress.set(false);
        }
    }

    @Scheduled(every = "${pg-facade.orchestration.common.standby.count-check-interval}")
    public void checkStandbyCount() {
        if (orchestratorReady.get() && !switchoverInProgress.get() && !clusterRestartInProgress.get() && standbyCountCheckInProgress.compareAndSet(false, true)) {
            List<PostgresAdapterInstanceInfo> availableInstances = orchestrationAdapter.get().getAvailablePostgresInstancesInfos();
            checkAndFixStandbyCount(availableInstances);
            standbyCountCheckInProgress.set(false);
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
                clusterRuntimeProperties.getAllPostgresInstancesInfos().values().forEach(
                        instance -> {
                            postgresConfigurator.changePostgresSettings(instance.getInstanceId(), mergedSettings);
                        }
                );
                postgresPersistedProperties.savePostgresSettingsInfos(mergedSettings);
            } catch (Exception e) {
                log.error("Failed to apply required settings default values ", e);
            }
        }
    }

    private void addInstanceToRuntimeProperties(PostgresAdapterInstanceInfo instanceInfo) {
        clusterRuntimeProperties.getAllPostgresInstancesInfos().put(
                instanceInfo.getInstanceId(),
                RuntimePostgresInstanceInfo
                        .builder()
                        .primary(instanceInfo.getMaster())
                        .address(instanceInfo.getInstanceAddress())
                        .port(instanceInfo.getInstancePort())
                        .instanceId(instanceInfo.getInstanceId())
                        .build()
        );
    }

    private void checkPrimaryHealthAndFailoverIfNeeded(List<PostgresAdapterInstanceInfo> availableInstances) {
        boolean foundMaster = false;
        boolean healthcheckFailed = false;
        PostgresAdapterInstanceInfo newMasterInstanceInfo = null;

        for (PostgresAdapterInstanceInfo instanceInfo : availableInstances) {
            if (instanceInfo.getMaster()) {
                foundMaster = true;
            }
            if (instanceInfo.getMaster() && !InstanceHealth.HEALTHY.equals(instanceInfo.getHealth())) {
                healthcheckFailedCount++;
                healthcheckFailed = true;
                log.error("POSTGRES PRIMARY UNHEALTHY. {} HEALTHCHECKS FAILED WHEN MAXIMUM IS {}", healthcheckFailedCount, orchestrationProperties.common().postgresDeadCheck().retries());
            }
        }

        if (!foundMaster) {
            log.error("NO ACTIVE PRIMARY FOUND. NEED TO FAILOVER.");
            newMasterInstanceInfo = selectNewPrimary(availableInstances);
            if (newMasterInstanceInfo == null) {
                log.error("FATAL ERROR. NO PRIMARY FOUND BUT NO HEALTH AND ACTIVE STANDBY FOUND. CAN NOT FAILOVER!!!");
            }
        }

        if (healthcheckFailedCount >= orchestrationProperties.common().postgresDeadCheck().retries()) {
            log.error("REACHED MAXIMUM HEALTHCHECKS RETRY COUNT. NEED TO FAILOVER.");
            newMasterInstanceInfo = selectNewPrimary(availableInstances);
            if (newMasterInstanceInfo == null) {
                log.error("FATAL ERROR. POSTGRES PRIMARY IS UNHEALTHY BUT NO ALIVE AND ACTIVE STANDBY FOUND. CAN NOT FAILOVER!!!");
            }
        }

        if (newMasterInstanceInfo != null) {
            log.info("FAILOVER STARTED. STANDBY WILL SWITCHOVER PRIMARY.");
            if (!switchover(newMasterInstanceInfo)) {
                log.error("FATAL ERROR. TRIED TO PROMOTE STANDBY BUT FAILED. FAILOVER FAILED!!!");
                return;
            } else {
                waitUntilPostgresInstanceHealthy(newMasterInstanceInfo.getInstanceId());
                healthcheckFailedCount = 0;
            }
        }

        if (!healthcheckFailed) {
            healthcheckFailedCount = 0;
        }
    }

    private PostgresAdapterInstanceInfo selectNewPrimary(List<PostgresAdapterInstanceInfo> availableInstances) {
        //TODO maybe add some better logic to select new primary. By LSN?
        return availableInstances
                .stream()
                .filter(info ->
                        Boolean.FALSE.equals(info.getMaster())
                                && InstanceHealth.HEALTHY.equals(info.getHealth())
                                && InstanceStatus.ACTIVE.equals(info.getStatus())
                )
                .filter(info -> !restartingStandbyInstanceIds.contains(info.getInstanceId()))
                .findFirst()
                .orElse(null);
    }

    private void checkAndFixStandbyCount(List<PostgresAdapterInstanceInfo> availableInstances) {
        if (clusterRestartInProgress.get()) {
            return;
        }

        List<PostgresAdapterInstanceInfo> healthyOrStartingInstances = availableInstances
                .stream()
                .filter(info -> Boolean.FALSE.equals(info.getMaster()))
                .filter(info ->
                        InstanceHealth.HEALTHY.equals(info.getHealth())
                                || InstanceHealth.STARTING.equals(info.getHealth())
                )
                .toList();

        long countDiff = orchestrationProperties.common().standby().count() - healthyOrStartingInstances.size();

        if (healthyOrStartingInstances.size() < orchestrationProperties.common().standby().count()) {
            log.warn("Found {} starting or healthy standby while it is required to have {}. Need to start {} more.",
                    healthyOrStartingInstances.size(),
                    orchestrationProperties.common().standby().count(),
                    countDiff
            );

            List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();

            for (int i = 0; i < countDiff; i++) {
                completableFutureList.add(managedExecutor.runAsync(() -> {
                            PostgresAdapterInstanceInfo instanceInfo = createStartAndWaitForNewInstanceToBeReady(false);
                            addInstanceToRuntimeProperties(instanceInfo);
                            log.info("Standby is up and running!");
                        })
                );
            }

            try {
                CompletableFuture.allOf(completableFutureList.toArray(new CompletableFuture[0])).join();
            } catch (Exception e) {
                log.error("Error while scaling standby count", e);
            }
        } else if (healthyOrStartingInstances.size() > orchestrationProperties.common().standby().count()) {
            log.warn("Found {} starting or healthy standby while it is required to have {}. Will stop {} instance(s).",
                    healthyOrStartingInstances.size(),
                    orchestrationProperties.common().standby().count(),
                    Math.abs(countDiff)
            );

            for (int i = 0; i < Math.abs(countDiff); i++) {
                orchestrationAdapter.get().deletePostgresInstance(
                        healthyOrStartingInstances.get(i).getInstanceId(),
                        false
                );
            }
        }

        //remove unhealthy or inactive standby
        availableInstances
                .stream()
                .filter(info -> !restartingStandbyInstanceIds.contains(info.getInstanceId()))
                .filter(info -> Boolean.FALSE.equals(info.getMaster()))
                .filter(info ->
                        InstanceHealth.UNHEALTHY.equals(info.getHealth())
                                || InstanceStatus.NOT_ACTIVE.equals(info.getStatus())
                )
                .forEach(info -> {
                    log.info("Found unhealthy or inactive standby. Removing it.");
                    removeStandby(info);
                });
    }

    private void removeStandby(PostgresAdapterInstanceInfo instanceInfo) {
        UUID eventId = UUID.randomUUID();

        standbyRemoveStartedEvent.fire(
                new StandbyRemoveStartedEvent(
                        eventId,
                        instanceInfo.getInstanceId(),
                        instanceInfo.getInstanceAddress()
                )
        );

        clusterRuntimeProperties.getAllPostgresInstancesInfos().remove(instanceInfo.getInstanceId());

        StandbyRemoveCompletedEvent completedEvent = new StandbyRemoveCompletedEvent(
                eventId,
                instanceInfo.getInstanceId(),
                true
        );

        completedEvent.setSuccess(orchestrationAdapter.get().deletePostgresInstance(instanceInfo.getInstanceId(), true));

        standbyRemoveCompletedEvent.fire(completedEvent);
    }

    private synchronized boolean switchover(PostgresAdapterInstanceInfo newPrimaryInstanceInfo) {
        UUID switchoverEventId = UUID.randomUUID();
        UUID removeStandbyEventId = UUID.randomUUID();

        switchoverInProgress.set(true);

        try {
            switchoverStartedEvent.fire(new SwitchoverStartedEvent(switchoverEventId));
            standbyRemoveStartedEvent.fire(new StandbyRemoveStartedEvent(removeStandbyEventId, newPrimaryInstanceInfo.getInstanceId(), newPrimaryInstanceInfo.getInstanceAddress()));
            log.info("SWITCHOVER STARTED. SWITCHING TO INSTANCE WITH IP {} AND PORT {}", newPrimaryInstanceInfo.getInstanceAddress(), newPrimaryInstanceInfo.getInstancePort());

            Connection standbyConnection = postgresUtils.getConnectionForPgFacadeUser(
                    newPrimaryInstanceInfo.getInstanceAddress(),
                    newPrimaryInstanceInfo.getInstancePort()
            );

            ResultSet promoteResultSet = standbyConnection.createStatement().executeQuery("SELECT pg_promote()");
            promoteResultSet.next();
            boolean promoteSuccessful = promoteResultSet.getBoolean(1);
            if (!promoteSuccessful) {
                log.error("ERROR DURING SWITCHOVER. STANDBY CANT COMPLETE PROMOTE REQUEST.");
                return false;
            }

            standbyConnection.close();

            clusterRuntimeProperties.getAllPostgresInstancesInfos().remove(masterInstanceId);
            newPrimaryInstanceInfo.setMaster(true);
            addInstanceToRuntimeProperties(newPrimaryInstanceInfo);
            orchestrationAdapter.get().updateInstancesAfterSwitchover(newPrimaryInstanceInfo.getInstanceId(), masterInstanceId);

            masterInstanceId = newPrimaryInstanceInfo.getInstanceId();

            //remove all standby because of new timeline
            //TODO it is possible to repair such nodes instead of deleting them. Use recovery_target_timeline = 'latest'
            log.info("NEW PRIMARY PROMOTED. REMOVING ALL FORMER STANDBY BECAUSE OF NEW TIMELINE.");
            orchestrationAdapter.get().getAvailablePostgresInstancesInfos()
                    .stream()
                    .filter(info -> Boolean.FALSE.equals(info.getMaster()))
                    .forEach(this::removeStandby);

            switchoverCompletedEvent.fire(new SwitchoverCompletedEvent(switchoverEventId, true));
            standbyRemoveCompletedEvent.fire(new StandbyRemoveCompletedEvent(removeStandbyEventId, newPrimaryInstanceInfo.getInstanceId(), true));
            clusterRestartInProgress.set(false);
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

    private PostgresAdapterInstanceInfo restartStandbyAndWaitUntilItIsReady(UUID standbyInstanceId) throws InstanceRestartException, AwaitHealthyInstanceException {
        PostgresAdapterInstanceInfo standbyInfo = orchestrationAdapter.get().getInstanceInfo(standbyInstanceId);
        if (standbyInfo == null || standbyInfo.getMaster()) {
            throw new InstanceRestartException("Master can not be restarted, only switchover is possible. Most likely its a bug, please report it.");
        }
        restartingStandbyInstanceIds.add(standbyInstanceId);
        UUID restartEventId = UUID.randomUUID();
        clusterRuntimeProperties.getAllPostgresInstancesInfos().remove(standbyInstanceId);
        standbyRestartStartedEvent.fire(new StandbyRestartStartedEvent(restartEventId, standbyInstanceId, standbyInfo.getInstanceAddress()));

        try {
            if (!orchestrationAdapter.get().restartPostgresInstance(standbyInstanceId)) {
                throw new InstanceRestartException("Failed to restart instance.");
            }

            standbyInfo = waitUntilPostgresInstanceHealthy(standbyInstanceId);

            addInstanceToRuntimeProperties(standbyInfo);
            standbyRestartCompletedEvent.fire(new StandbyRestartCompletedEvent(restartEventId, standbyInstanceId, true));
            return standbyInfo;
        } catch (Exception e) {
            standbyRestartCompletedEvent.fire(new StandbyRestartCompletedEvent(restartEventId, standbyInstanceId, false));
            restartingStandbyInstanceIds.remove(standbyInstanceId);
            throw e;
        }
    }

    private PostgresAdapterInstanceInfo createStartAndWaitForNewInstanceToBeReady(boolean primary) throws InstanceCreationException, AwaitHealthyInstanceException {
        UUID instanceId = orchestrationAdapter.get().createNewPostgresInstance(
                PostgresInstanceCreationRequest
                        .builder()
                        .primary(primary)
                        .standbySettings(
                                primary
                                        ? null
                                        : postgresPersistedProperties.getPostgresSettingInfos()
                        )
                        .build()
        );

        if (instanceId == null) {
            throw new InstanceCreationException("Can not create new Postgres instance.");
        }

        boolean started = orchestrationAdapter.get().startPostgresInstance(instanceId);

        if (!started) {
            throw new InstanceCreationException("Can not start new Postgres instance");
        }

        log.info("Started new instance. Will wait until it is healthy...");

        return waitUntilPostgresInstanceHealthy(instanceId);
    }

    private PostgresAdapterInstanceInfo waitUntilPostgresInstanceHealthy(UUID instanceId) throws AwaitHealthyInstanceException {
        OrchestrationProperties.CommonProperties.PostgresStartupCheckProperties startupCheckProperties = orchestrationProperties.common().postgresStartupCheck();

        long endTime = System.currentTimeMillis() + (startupCheckProperties.interval() * startupCheckProperties.retries()) + startupCheckProperties.startPeriod();
        PostgresAdapterInstanceInfo instanceInfo = orchestrationAdapter.get().getInstanceInfo(instanceId);

        try {
            while (!InstanceHealth.HEALTHY.equals(instanceInfo.getHealth()) && endTime > System.currentTimeMillis()) {
                Thread.sleep(startupCheckProperties.interval());
                instanceInfo = orchestrationAdapter.get().getInstanceInfo(instanceId);
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
}
