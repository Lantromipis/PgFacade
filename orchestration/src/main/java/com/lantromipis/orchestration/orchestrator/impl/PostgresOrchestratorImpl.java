package com.lantromipis.orchestration.orchestrator.impl;

import com.lantromipis.configuration.event.MasterReadyEvent;
import com.lantromipis.configuration.event.SwitchoverCompletedEvent;
import com.lantromipis.configuration.event.SwitchoverStartedEvent;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.OrchestrationAdapter;
import com.lantromipis.orchestration.exception.InstanceCreationException;
import com.lantromipis.orchestration.model.InstanceHealth;
import com.lantromipis.orchestration.model.InstanceStatus;
import com.lantromipis.orchestration.model.PostgresInstanceCreationRequest;
import com.lantromipis.orchestration.model.PostgresAdapterInstanceInfo;
import com.lantromipis.orchestration.orchestrator.api.PostgresConfigurator;
import com.lantromipis.orchestration.orchestrator.api.PostgresOrchestrator;
import com.lantromipis.orchestration.util.PostgresUtils;
import io.quarkus.scheduler.Scheduled;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@ApplicationScoped
public class PostgresOrchestratorImpl implements PostgresOrchestrator {
    @Inject
    OrchestrationAdapter orchestrationAdapter;

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
    PostgresConfigurator postgresConfigurator;

    @Inject
    ManagedExecutor managedExecutor;

    @Inject
    PostgresUtils postgresUtils;

    private AtomicBoolean orchestratorReady = new AtomicBoolean(false);
    private AtomicBoolean failoverCheckInProgress = new AtomicBoolean(false);

    private UUID masterInstanceId;

    @Override
    public void initialize() {
        orchestrationAdapter.initialize();

        //master section
        PostgresAdapterInstanceInfo masterInstanceInfo = orchestrationAdapter.getAvailablePostgresInstancesInfos()
                .stream()
                .filter(info -> Boolean.TRUE.equals(info.getMaster()))
                .findFirst()
                .orElse(null);

        boolean isNewDBSetup = false;

        if (masterInstanceInfo == null) {
            log.info("Can not find any Postgres master instance. Will create and start new one.");
            masterInstanceInfo = createStartAndWaitForNewInstanceToBeReady(true);
            isNewDBSetup = true;
        } else if (InstanceStatus.NOT_ACTIVE.equals(masterInstanceInfo.getStatus())) {
            log.info("Found non-active Postgres master instance. Will start it now.");
            boolean masterStarted = orchestrationAdapter.startPostgresInstance(masterInstanceInfo.getInstanceId());

            if (!masterStarted) {
                log.info("Can not start non-active master instance. Will delete it and create and start new one.");
                orchestrationAdapter.deletePostgresInstance(masterInstanceInfo.getInstanceId());
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
        List<PostgresAdapterInstanceInfo> standbyInfos = orchestrationAdapter.getAvailablePostgresInstancesInfos()
                .stream()
                .filter(info -> Boolean.FALSE.equals(info.getMaster()))
                .toList();

        log.info("Found {} standby instances while it is required to have {}", standbyInfos.size(), orchestrationProperties.common().standByCount());

        long countDiff = orchestrationProperties.common().standByCount() - standbyInfos.size();

        if (countDiff > 0) {
            log.info("Found less standby instances than required. Will create and start {} instances", countDiff);

            List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();
            for (int i = 0; i < Math.abs(countDiff); i++) {
                completableFutureList.add(
                        managedExecutor.runAsync(() -> {
                            createStartAndWaitForNewInstanceToBeReady(false);
                            log.info("Standby is up and running!");
                        })
                );
            }

            CompletableFuture.allOf(completableFutureList.toArray(new CompletableFuture[0])).join();
        } else {
            log.info("Found all required standby instances. Will start instances if needed.");

            for (int i = 0; i < orchestrationProperties.common().standByCount(); i++) {
                PostgresAdapterInstanceInfo standbyInfo = standbyInfos.get(i);
                if (InstanceStatus.ACTIVE.equals(standbyInfo.getStatus())) {
                    log.info("Standby already active. No actions needed.");
                } else {
                    log.info("Starting standby...");
                    try {
                        orchestrationAdapter.startPostgresInstance(standbyInfo.getInstanceId());
                        log.info("Started standby. Will wait until it is healthy...");
                        waitUntilPostgresInstanceHealthy(standbyInfo.getInstanceId());
                        log.info("Standby is up ad running!");
                    } catch (Exception e) {
                        log.error("Failed to start standby", e);
                        //TODO do something? or will that do scheduler?
                    }
                }
            }
        }

        orchestratorReady.set(true);
    }

    @Override
    public void switchover(UUID newMasterInstanceId) {
        switchover(orchestrationAdapter.getInstanceInfo(newMasterInstanceId));
    }

    @Scheduled(every = "${pg-facade.orchestration.common.postgres-dead-check.interval}")
    public void checkPostgresLiveliness() {
        if (orchestratorReady.get() && failoverCheckInProgress.compareAndSet(false, true)) {
            List<PostgresAdapterInstanceInfo> availableInstances = orchestrationAdapter.getAvailablePostgresInstancesInfos();

            boolean foundMaster = false;
            PostgresAdapterInstanceInfo newMasterInstanceInfo = null;
            //check master health
            for (PostgresAdapterInstanceInfo instanceInfo : availableInstances) {
                if (instanceInfo.getMaster()) {
                    foundMaster = true;
                }
                if (instanceInfo.getMaster() && !InstanceHealth.HEALTHY.equals(instanceInfo.getHealth())) {
                    log.error("POSTGRES PRIMARY UNHEALTHY. WILL FAILOVER.");
                    newMasterInstanceInfo = selectNewMaster(availableInstances);

                    if (newMasterInstanceInfo == null) {
                        log.error("FATAL ERROR. POSTGRES PRIMARY IS UNHEALTHY BUT NO ALIVE AND ACTIVE STANDBY FOUND. CAN NOT FAILOVER!!!");
                    }
                }
            }

            if (!foundMaster) {
                log.error("NO ACTIVE MASTER FOUND. WILL FAILOVER.");
                newMasterInstanceInfo = selectNewMaster(availableInstances);
            }

            if (newMasterInstanceInfo != null) {
                if (!switchover(newMasterInstanceInfo)) {
                    log.error("FATAL ERROR. TRIED TO PROMOTE STANDBY BUT FAILED.");
                    return;
                } else {
                    waitUntilPostgresInstanceHealthy(newMasterInstanceInfo.getInstanceId());
                }
            }
        }

        failoverCheckInProgress.set(false);
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

    private synchronized boolean switchover(PostgresAdapterInstanceInfo newMasterInstanceInfo) {
        UUID switchoverEventId = UUID.randomUUID();

        try {
            masterSwitchoverStartedEvent.fire(new SwitchoverStartedEvent(switchoverEventId));
            log.info("SWITCHOVER STARTED. SWITCHING TO INSTANCE WITH IP {} AND PORT {}", newMasterInstanceInfo.getInstanceAddress(), newMasterInstanceInfo.getInstancePort());

            Connection standbyConnection = postgresUtils.getConnectionForPgFacadeUser(
                    newMasterInstanceInfo.getInstanceAddress(),
                    newMasterInstanceInfo.getInstancePort()
            );

            ResultSet promoteResultSet = standbyConnection.createStatement()
                    .executeQuery("SELECT pg_promote()");
            promoteResultSet.next();
            boolean promoteSuccessful = promoteResultSet.getBoolean(1);
            if (!promoteSuccessful) {
                log.error("ERROR DURING SWITCHOVER. STANDBY CANT COMPLETE PROMOTE REQUEST.");
                return false;
            }

            standbyConnection.close();

            orchestrationAdapter.deletePostgresInstance(masterInstanceId);
            clusterRuntimeProperties.setMasterHostAddress(newMasterInstanceInfo.getInstanceAddress());
            clusterRuntimeProperties.setMasterPort(newMasterInstanceInfo.getInstancePort());
            orchestrationAdapter.updateInstancesAfterSwitchover(newMasterInstanceInfo.getInstanceId(), masterInstanceId);

            masterInstanceId = newMasterInstanceInfo.getInstanceId();

            masterSwitchoverEvent.fire(new SwitchoverCompletedEvent(switchoverEventId, true));
            log.info("SWITCHOVER COMPLETED SUCCESSFULLY");
            return true;
        } catch (Exception e) {
            masterSwitchoverEvent.fire(new SwitchoverCompletedEvent(switchoverEventId, false));
            log.error("ERROR DURING SWITCHOVER", e);
            return false;
        }
    }

    private PostgresAdapterInstanceInfo createStartAndWaitForNewInstanceToBeReady(boolean master) {
        UUID instanceId = orchestrationAdapter.createNewPostgresInstance(
                PostgresInstanceCreationRequest
                        .builder()
                        .master(master)
                        .postgresqlSettings(
                                Map.of("shared_buffers", "256MB") //TODO constant for test
                        )
                        .build()
        );

        if (instanceId == null) {
            throw new InstanceCreationException("Can not create new Postgres instance.");
        }

        boolean started = orchestrationAdapter.startPostgresInstance(instanceId);

        if (!started) {
            throw new InstanceCreationException("Can not start new Postgres instance");
        }

        log.info("Started new instance. Will wait until it is healthy...");

        return waitUntilPostgresInstanceHealthy(instanceId);
    }

    private PostgresAdapterInstanceInfo waitUntilPostgresInstanceHealthy(UUID instanceId) {
        OrchestrationProperties.CommonProperties.PostgresStartupCheckProperties startupCheckProperties = orchestrationProperties.common().postgresStartupCheck();

        long endTime = System.currentTimeMillis() + (startupCheckProperties.interval() * startupCheckProperties.retries()) + startupCheckProperties.startPeriod();
        PostgresAdapterInstanceInfo instanceInfo = orchestrationAdapter.getInstanceInfo(instanceId);

        try {
            while (!InstanceHealth.HEALTHY.equals(instanceInfo.getHealth()) && endTime > System.currentTimeMillis()) {
                Thread.sleep(startupCheckProperties.interval());
                instanceInfo = orchestrationAdapter.getInstanceInfo(instanceId);
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
