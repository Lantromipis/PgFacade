package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.model.PgFacadeRaftNodeInfo;
import com.lantromipis.orchestration.service.api.PgFacadeOrchestrator;
import com.lantromipis.orchestration.service.api.PgFacadeRaftService;
import com.lantromipis.orchestration.util.RaftFunctionalityCombinator;
import io.quarkus.scheduler.Scheduled;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@ApplicationScoped
public class PgFacadeOrchestratorImpl implements PgFacadeOrchestrator {
    @Inject
    Instance<PlatformAdapter> platformAdapter;

    @Inject
    PgFacadeRaftService pgFacadeRaftService;

    @Inject
    ManagedExecutor managedExecutor;

    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    @Inject
    RaftFunctionalityCombinator raftFunctionalityCombinator;

    @Override
    public void startOrchestration() {
        log.info("Starting PgFacade orchestration!");
    }

    @Scheduled(every = "${pg-facade.raft.nodes-count-check-interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void checkStandbyCount() {
        if (PgFacadeRaftRole.LEADER.equals(pgFacadeRuntimeProperties.getRaftRole())) {
            if (platformAdapter.get().getActiveRaftNodeInfos().size() < 3) {
                PgFacadeRaftNodeInfo raftNodeInfo = platformAdapter.get().createAndStartNewPgFacadeInstance();
                pgFacadeRaftService.addNewRaftNode(raftNodeInfo);
            }
        }
    }

    @Scheduled(every = "PT0.5S")
    public void test() {
        if (PgFacadeRaftRole.LEADER.equals(pgFacadeRuntimeProperties.getRaftRole())) {
            int count = ThreadLocalRandom.current().nextInt(8 - 3) + 3;
            for (int i = 0; i < count; i++) {
                raftFunctionalityCombinator.testIfAbleToCommitToRaftNoException();
            }
        }
    }
}
