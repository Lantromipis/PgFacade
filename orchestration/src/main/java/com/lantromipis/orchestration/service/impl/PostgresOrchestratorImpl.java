package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.event.*;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.OrchestrationAdapter;
import com.lantromipis.orchestration.exception.InstanceCreationException;
import com.lantromipis.orchestration.exception.OrchestratorNotReadyException;
import com.lantromipis.orchestration.exception.PostgresConfigurationChangeException;
import com.lantromipis.orchestration.model.InstanceHealth;
import com.lantromipis.orchestration.model.InstanceStatus;
import com.lantromipis.orchestration.model.PostgresInstanceCreationRequest;
import com.lantromipis.orchestration.model.PostgresAdapterInstanceInfo;
import com.lantromipis.orchestration.service.api.PostgresConfigurator;
import com.lantromipis.orchestration.service.api.PostgresOrchestrator;
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
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@ApplicationScoped
public class PostgresOrchestratorImpl implements PostgresOrchestrator {
    @Inject
    Instance<OrchestrationAdapter> orchestrationAdapter;

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    OrchestrationProperties orchestrationProperties;

    @Inject
    Event<MasterReadyEvent> masterReadyEvent;

    @Inject
    Event<SwitchoverCompletedEvent> masterSwitchoverEvent;

    @Inject
    Event<SwitchoverStartedEvent> masterSwitchoverStartedEvent;

    @Inject
    Event<StandbyRemoveStartedEvent> standbyRemoveStartedEvent;

    @Inject
    Event<StandbyRemoveCompletedEvent> standbyRemoveCompletedEvent;

    @Inject
    PostgresConfigurator postgresConfigurator;

    @Inject
    ManagedExecutor managedExecutor;

    @Inject
    PostgresUtils postgresUtils;

    @Inject
    PostgresProperties postgresProperties;

    private final AtomicBoolean orchestratorReady = new AtomicBoolean(false);
    private final AtomicBoolean livelinessCheckInProgress = new AtomicBoolean(false);
    private final AtomicBoolean standbyCountCheckInProgress = new AtomicBoolean(false);
    private final AtomicBoolean switchoverInProgress = new AtomicBoolean(false);
    private final AtomicBoolean clusterRestartInProgress = new AtomicBoolean(false);
    private int healthcheckFailedCount = 0;

    private UUID masterInstanceId;

    @Override
    public void initialize() {
        if (OrchestrationProperties.AdapterType.NO_ADAPTER.equals(orchestrationProperties.adapter())) {
            log.info("No orchestrator adapter is configured. PgFacade will work like proxy + connection pool without any HA features for Postgres. Consider choosing another adapter if you need HA features.");
            return;
        }

        orchestrationAdapter.get().initialize();

        //master section
        PostgresAdapterInstanceInfo masterInstanceInfo = orchestrationAdapter.get().getAvailablePostgresInstancesInfos().stream().filter(info -> Boolean.TRUE.equals(info.getMaster())).findFirst().orElse(null);

        boolean isNewDBSetup = false;

        if (masterInstanceInfo == null) {
            log.info("Can not find any Postgres master instance. Will create and start new one.");
            masterInstanceInfo = createStartAndWaitForNewInstanceToBeReady(true);
            isNewDBSetup = true;
        } else if (InstanceStatus.NOT_ACTIVE.equals(masterInstanceInfo.getStatus())) {
            log.info("Found non-active Postgres master instance. Will start it now.");
            boolean masterStarted = orchestrationAdapter.get().startPostgresInstance(masterInstanceInfo.getInstanceId());

            if (!masterStarted) {
                log.info("Can not start non-active master instance. Will delete it and create and start new one.");
                orchestrationAdapter.get().deletePostgresInstance(masterInstanceInfo.getInstanceId(), true);
                masterInstanceInfo = createStartAndWaitForNewInstanceToBeReady(true);
                isNewDBSetup = true;
            } else {
                masterInstanceInfo = waitUntilPostgresInstanceHealthy(masterInstanceInfo.getInstanceId());
            }
        } else if (InstanceStatus.ACTIVE.equals(masterInstanceInfo.getStatus())) {
            log.info("Found active Postgres master. No actions needed.");
        }

        log.info("Master is up and running!");

        clusterRuntimeProperties.setMasterHostAddress(masterInstanceInfo.getInstanceAddress());
        clusterRuntimeProperties.setMasterPort(masterInstanceInfo.getInstancePort());

        if (isNewDBSetup) {
            postgresConfigurator.configureNewlyCreatedMaster();
        }

        masterInstanceId = masterInstanceInfo.getInstanceId();
        masterReadyEvent.fire(new MasterReadyEvent());

        //standby section
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

        postgresConfigurator.initialize();

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
    public void switchover(UUID newMasterInstanceId) {
        if (orchestratorReady.get()) {
            switchover(orchestrationAdapter.get().getInstanceInfo(newMasterInstanceId));
        }
    }

    @Override
    public void changePostgresSettings(Map<String, String> newSettingNamesAndValuesMap) throws PostgresConfigurationChangeException {
        if (orchestratorReady.get() && clusterRestartInProgress.compareAndSet(false, true)) {
            try {
                //TODO persist values
                boolean restartRequired = postgresConfigurator.changePostgresSettings(null, newSettingNamesAndValuesMap);
            } catch (Exception e) {
                throw e;
            } finally {
                clusterRestartInProgress.set(false);
            }

            clusterRestartInProgress.set(false);
        } else if (clusterRestartInProgress.get()) {
            throw new PostgresConfigurationChangeException("Currently restarting cluster due to settings changes. Try again later.");
        } else {
            throw new OrchestratorNotReadyException("Orchestrator not ready. Its initialization is still in progress or PgFacade is configured to work just like proxy.");
        }
    }

    @Scheduled(every = "${pg-facade.orchestration.common.postgres-dead-check.interval}")
    public void checkPrimaryLiveliness() {
        if (orchestratorReady.get() && livelinessCheckInProgress.compareAndSet(false, true)) {
            List<PostgresAdapterInstanceInfo> availableInstances = orchestrationAdapter.get().getAvailablePostgresInstancesInfos();
            checkPrimaryHealthAndFailoverIfNeeded(availableInstances);
            livelinessCheckInProgress.set(false);
        }
    }

    @Scheduled(every = "${pg-facade.orchestration.common.standby.count-check-interval}")
    public void checkStandbyCount() {
        if (orchestratorReady.get() && !switchoverInProgress.get() && standbyCountCheckInProgress.compareAndSet(false, true)) {
            List<PostgresAdapterInstanceInfo> availableInstances = orchestrationAdapter.get().getAvailablePostgresInstancesInfos();
            checkAndFixStandbyCount(availableInstances);
            standbyCountCheckInProgress.set(false);
        }
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
            newMasterInstanceInfo = selectNewMaster(availableInstances);
            if (newMasterInstanceInfo == null) {
                log.error("FATAL ERROR. NO PRIMARY FOUND BUT NO HEALTH AND ACTIVE STANDBY FOUND. CAN NOT FAILOVER!!!");
            }
        }

        if (healthcheckFailedCount >= orchestrationProperties.common().postgresDeadCheck().retries()) {
            log.error("REACHED MAXIMUM HEALTHCHECKS RETRY COUNT. NEED TO FAILOVER.");
            newMasterInstanceInfo = selectNewMaster(availableInstances);
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

    private PostgresAdapterInstanceInfo selectNewMaster(List<PostgresAdapterInstanceInfo> availableInstances) {
        //TODO maybe add some better logic to select new master
        return availableInstances
                .stream()
                .filter(info ->
                        Boolean.FALSE.equals(info.getMaster())
                                && InstanceHealth.HEALTHY.equals(info.getHealth())
                                && InstanceStatus.ACTIVE.equals(info.getStatus())
                )
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
                            createStartAndWaitForNewInstanceToBeReady(false);
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

        StandbyRemoveCompletedEvent completedEvent = new StandbyRemoveCompletedEvent(
                eventId,
                instanceInfo.getInstanceId(),
                instanceInfo.getInstanceAddress(),
                true
        );

        completedEvent.setSuccess(orchestrationAdapter.get().deletePostgresInstance(instanceInfo.getInstanceId(), true));

        standbyRemoveCompletedEvent.fire(completedEvent);
    }

    private synchronized boolean switchover(PostgresAdapterInstanceInfo newMasterInstanceInfo) {
        UUID switchoverEventId = UUID.randomUUID();

        switchoverInProgress.set(true);

        try {
            masterSwitchoverStartedEvent.fire(new SwitchoverStartedEvent(switchoverEventId));
            log.info("SWITCHOVER STARTED. SWITCHING TO INSTANCE WITH IP {} AND PORT {}", newMasterInstanceInfo.getInstanceAddress(), newMasterInstanceInfo.getInstancePort());

            Connection standbyConnection = postgresUtils.getConnectionForPgFacadeUser(newMasterInstanceInfo.getInstanceAddress(), newMasterInstanceInfo.getInstancePort());

            ResultSet promoteResultSet = standbyConnection.createStatement().executeQuery("SELECT pg_promote()");
            promoteResultSet.next();
            boolean promoteSuccessful = promoteResultSet.getBoolean(1);
            if (!promoteSuccessful) {
                log.error("ERROR DURING SWITCHOVER. STANDBY CANT COMPLETE PROMOTE REQUEST.");
                return false;
            }

            standbyConnection.close();

            clusterRuntimeProperties.setMasterHostAddress(newMasterInstanceInfo.getInstanceAddress());
            clusterRuntimeProperties.setMasterPort(newMasterInstanceInfo.getInstancePort());
            orchestrationAdapter.get().updateInstancesAfterSwitchover(newMasterInstanceInfo.getInstanceId(), masterInstanceId);

            masterInstanceId = newMasterInstanceInfo.getInstanceId();

            //remove all standby because of new timeline
            //TODO it is possible to repair such nodes instead of deleting them. Use recovery_target_timeline = 'latest'
            log.info("NEW PRIMARY PROMOTED. REMOVING ALL FORMER STANDBY BECAUSE OF NEW TIMELINE.");
            orchestrationAdapter.get().getAvailablePostgresInstancesInfos()
                    .stream()
                    .filter(info -> Boolean.FALSE.equals(info.getMaster()))
                    .forEach(this::removeStandby);

            masterSwitchoverEvent.fire(new SwitchoverCompletedEvent(switchoverEventId, true));
            clusterRestartInProgress.set(false);
            log.info("SWITCHOVER COMPLETED SUCCESSFULLY");
            return true;
        } catch (Exception e) {
            masterSwitchoverEvent.fire(new SwitchoverCompletedEvent(switchoverEventId, false));
            log.error("ERROR DURING SWITCHOVER", e);
            return false;
        } finally {
            switchoverInProgress.set(false);
        }
    }

    private PostgresAdapterInstanceInfo createStartAndWaitForNewInstanceToBeReady(boolean master) {
        UUID instanceId = orchestrationAdapter.get().createNewPostgresInstance(
                PostgresInstanceCreationRequest
                        .builder()
                        .master(master)
                        .postgresqlSettings(
                                postgresProperties.postgresqlConfOverride()
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

    private PostgresAdapterInstanceInfo waitUntilPostgresInstanceHealthy(UUID instanceId) {
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
            throw new InstanceCreationException("Failed to achieve healthy instance.");
        }

        if (!InstanceHealth.HEALTHY.equals(instanceInfo.getHealth())) {
            throw new InstanceCreationException("Failed to achieve healthy instance.");
        }

        return instanceInfo;
    }
}
