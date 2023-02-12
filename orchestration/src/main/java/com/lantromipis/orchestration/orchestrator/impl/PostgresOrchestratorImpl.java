package com.lantromipis.orchestration.orchestrator.impl;

import com.lantromipis.configuration.event.MasterReadyEvent;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.configuration.properties.stored.api.PersistedProperties;
import com.lantromipis.orchestration.adapter.api.OrchestrationAdapter;
import com.lantromipis.orchestration.exception.InstanceCreationException;
import com.lantromipis.orchestration.model.InstanceHealth;
import com.lantromipis.orchestration.model.InstanceStatus;
import com.lantromipis.orchestration.model.PostgresInstanceCreationRequest;
import com.lantromipis.orchestration.model.PostgresInstanceInfo;
import com.lantromipis.orchestration.orchestrator.api.PostgresConfigurator;
import com.lantromipis.orchestration.orchestrator.api.PostgresOrchestrator;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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
    PostgresConfigurator postgresConfigurator;

    @Inject
    ManagedExecutor managedExecutor;

    @Override
    public void initialize() {
        orchestrationAdapter.initialize();

        //master section
        PostgresInstanceInfo masterInstanceInfo = orchestrationAdapter.getAvailablePostgresInstancesInfos()
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

        masterReadyEvent.fire(new MasterReadyEvent());


        //standby section
        List<PostgresInstanceInfo> standbyInfos = orchestrationAdapter.getAvailablePostgresInstancesInfos()
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
                PostgresInstanceInfo standbyInfo = standbyInfos.get(i);
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
    }

    private PostgresInstanceInfo createStartAndWaitForNewInstanceToBeReady(boolean master) {
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

    private PostgresInstanceInfo waitUntilPostgresInstanceHealthy(UUID instanceId) {
        OrchestrationProperties.CommonProperties.PostgresStartupCheckProperties startupCheckProperties = orchestrationProperties.common().postgresStartupCheck();

        long endTime = System.currentTimeMillis() + (startupCheckProperties.interval() * startupCheckProperties.retries()) + startupCheckProperties.startPeriod();
        PostgresInstanceInfo instanceInfo = orchestrationAdapter.getInstanceInfo(instanceId);

        try {
            while (!InstanceHealth.HEALTHY.equals(instanceInfo.getHealth()) && endTime > System.currentTimeMillis()) {
                instanceInfo = orchestrationAdapter.getInstanceInfo(instanceId);
                Thread.sleep(startupCheckProperties.interval());
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
