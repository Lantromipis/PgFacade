package com.lantromipis.orchestration.service.impl.raft;

import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.orchestration.service.api.PgFacadeOrchestrator;
import com.lantromipis.pgfacadeprotocol.server.api.RaftEventListener;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@Slf4j
@ApplicationScoped
public class RaftEventListenerImpl implements RaftEventListener {

    @Inject
    PgFacadeOrchestrator pgFacadeOrchestrator;

    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    @Override
    public void selfBecameLeader() {
        log.info("This PgFacade node is leader now! Starting orchestration...");
        pgFacadeRuntimeProperties.setRaftRole(PgFacadeRaftRole.LEADER);
        pgFacadeOrchestrator.startOrchestration();
    }

    @Override
    public void selfBecomeFollower() {
        log.info("This PgFacade node is not leader anymore!");
        pgFacadeRuntimeProperties.setRaftRole(PgFacadeRaftRole.FOLLOWER);
    }
}
