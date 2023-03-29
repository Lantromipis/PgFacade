package com.lantromipis.orchestration.service.impl;

import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.model.PgFacadeRaftNodeInfo;
import com.lantromipis.orchestration.service.api.PgFacadeOrchestrator;
import com.lantromipis.orchestration.service.api.PgFacadeRaftService;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

@Slf4j
@ApplicationScoped
public class PgFacadeOrchestratorImpl implements PgFacadeOrchestrator {
    @Inject
    Instance<PlatformAdapter> platformAdapter;

    @Inject
    PgFacadeRaftService pgFacadeRaftService;

    @Inject
    ManagedExecutor managedExecutor;

    @Override
    public void startOrchestration() {
        try {
            log.info("Starting PgFacade orchestration!");
            if (platformAdapter.get().getActiveRaftNodeInfos().size() < 2) {
                managedExecutor.runAsync(() -> {
                    try {
                        PgFacadeRaftNodeInfo raftNodeInfo = platformAdapter.get().createAndStartNewPgFacadeInstance();
                        pgFacadeRaftService.addNewRaftNode(raftNodeInfo);
                        PgFacadeRaftNodeInfo raftNodeInfo1 = platformAdapter.get().createAndStartNewPgFacadeInstance();
                        pgFacadeRaftService.addNewRaftNode(raftNodeInfo1);
                    } catch (Exception e) {
                        log.info("Failed", e);
                    }
                });


            }
        } catch (Exception e) {
            log.error("Failed to start PgFacade orchestration!", e);
        }
    }
}
