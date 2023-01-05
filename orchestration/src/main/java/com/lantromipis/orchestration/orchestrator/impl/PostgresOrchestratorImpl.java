package com.lantromipis.orchestration.orchestrator.impl;

import com.lantromipis.configuration.runtime.ClusterRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.OrchestrationAdapter;
import com.lantromipis.orchestration.model.InstanceStatus;
import com.lantromipis.orchestration.model.PostgresInstanceCreationRequest;
import com.lantromipis.orchestration.model.PostgresInstanceInfo;
import com.lantromipis.orchestration.orchestrator.api.PostgresOrchestrator;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@Slf4j
@ApplicationScoped
public class PostgresOrchestratorImpl implements PostgresOrchestrator {
    @Inject
    OrchestrationAdapter orchestrationAdapter;

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    public void initialize() {
        orchestrationAdapter.initialize();
        PostgresInstanceInfo masterInstanceInfo = orchestrationAdapter.getAvailablePostgresInstances().stream().filter(PostgresInstanceInfo::isMaster).findFirst().orElse(null);

        if (masterInstanceInfo == null) {
            log.info("Can not find active master. Will create new one.");
            masterInstanceInfo = orchestrationAdapter.createNewPostgresInstance(PostgresInstanceCreationRequest
                    .builder()
                    .master(true)//TODO constant until hot-standby!!!
                    .build()
            );
        } else if (InstanceStatus.NOT_ACTIVE.equals(masterInstanceInfo.getStatus())) {
            masterInstanceInfo = orchestrationAdapter.tryToStartPostgresInstance(masterInstanceInfo.getInstanceId());
        }

        clusterRuntimeProperties.setMasterHostAddress(masterInstanceInfo.getInstanceIpAddress());
    }
}
