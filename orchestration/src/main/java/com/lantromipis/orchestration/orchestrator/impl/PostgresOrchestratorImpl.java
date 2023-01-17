package com.lantromipis.orchestration.orchestrator.impl;

import com.lantromipis.configuration.event.MasterReadyEvent;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.OrchestrationAdapter;
import com.lantromipis.orchestration.exception.InstanceCreationException;
import com.lantromipis.orchestration.model.InstanceHealth;
import com.lantromipis.orchestration.model.InstanceStatus;
import com.lantromipis.orchestration.model.PostgresInstanceCreationRequest;
import com.lantromipis.orchestration.model.PostgresInstanceInfo;
import com.lantromipis.orchestration.orchestrator.api.PostgresConfigurator;
import com.lantromipis.orchestration.orchestrator.api.PostgresOrchestrator;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;
import java.util.Map;
import java.util.UUID;

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

    public void initialize() {
        orchestrationAdapter.initialize();

        PostgresInstanceInfo masterInstanceInfo = orchestrationAdapter.getAvailablePostgresInstancesInfos().stream().filter(PostgresInstanceInfo::isMaster).findFirst().orElse(null);

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

        log.info("Creating stand-by");

        UUID instanceId = orchestrationAdapter.createNewPostgresInstance(PostgresInstanceCreationRequest.builder().master(false).postgresqlSettings(Map.of("shared_buffers", "256MB")) //TODO constant for test
                .build());
        orchestrationAdapter.startPostgresInstance(instanceId);

        log.info("Started stand-by");
    }

    private PostgresInstanceInfo createStartAndWaitForNewInstanceToBeReady(boolean master) {
        UUID instanceId = orchestrationAdapter.createNewPostgresInstance(PostgresInstanceCreationRequest.builder().master(master).postgresqlSettings(Map.of("shared_buffers", "256MB")) //TODO constant for test
                .build());

        if (instanceId == null) {
            throw new InstanceCreationException("Can not create new Postgres instance.");
        }

        boolean started = orchestrationAdapter.startPostgresInstance(instanceId);

        if (!started) {
            throw new InstanceCreationException("Can not start new Postgres instance");
        }

        log.info("Created new instance. Will wait until it is healthy...");

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
