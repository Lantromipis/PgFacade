package com.lantromipis.orchestration.service.impl;

import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.model.PgFacadeRaftNodeInfo;
import com.lantromipis.orchestration.service.api.PgFacadeOrchestrator;
import com.lantromipis.orchestration.service.api.PgFacadeRaftService;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@Slf4j
@ApplicationScoped
public class PgFacadeOrchestratorImpl implements PgFacadeOrchestrator {
    @Inject
    PlatformAdapter platformAdapter;

    @Inject
    PgFacadeRaftService pgFacadeRaftService;


    @Override
    public void startOrchestration() {
        if (platformAdapter.getActiveRaftNodeInfos().size() < 2) {
            PgFacadeRaftNodeInfo raftNodeInfo = platformAdapter.createAndStartNewPgFacadeInstance();
            pgFacadeRaftService.addNewRaftNode(raftNodeInfo);
        }
    }
}
