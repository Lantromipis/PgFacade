package com.lantromipis.orchestrator.impl;

import com.lantromipis.adapter.api.OrchestrationAdapter;
import com.lantromipis.model.InstanceStatus;
import com.lantromipis.model.PostgresInstanceCreationRequest;
import com.lantromipis.model.PostgresInstanceInfo;
import com.lantromipis.orchestrator.api.PostgresOrchestrator;
import com.lantromipis.properties.runtime.ClusterRuntimeProperties;
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
