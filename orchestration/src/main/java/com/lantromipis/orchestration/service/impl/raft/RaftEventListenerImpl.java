package com.lantromipis.orchestration.service.impl.raft;

import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.orchestration.service.api.PgFacadeOrchestrator;
import com.lantromipis.pgfacadeprotocol.model.api.RaftRole;
import com.lantromipis.pgfacadeprotocol.server.api.RaftEventListener;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@Slf4j
@ApplicationScoped
public class RaftEventListenerImpl implements RaftEventListener {

    @Inject
    PgFacadeOrchestrator pgFacadeOrchestrator;

    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    @Inject
    ManagedExecutor managedExecutor;


    @Override
    public void syncedWithLeaderOrSelfIsLeaderOnStartup() {
        log.info("This PgFacade node is synced with PgFacade raft leader!");
    }

    @Override
    public void selfRoleChanged(RaftRole newRaftRole) {
        pgFacadeRuntimeProperties.setRaftRole(PgFacadeRaftRole.FOLLOWER);

        if (RaftRole.LEADER.equals(newRaftRole)) {
            log.info("This PgFacade node is leader now! Starting orchestration...");
            managedExecutor.runAsync(() -> {
                pgFacadeRuntimeProperties.setRaftRole(PgFacadeRaftRole.LEADER);
                pgFacadeOrchestrator.startOrchestration();
            });
        } else {
            log.info("This PgFacade node is not leader anymore!");
        }
    }
}
