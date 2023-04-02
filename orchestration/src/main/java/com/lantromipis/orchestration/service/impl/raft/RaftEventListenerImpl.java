package com.lantromipis.orchestration.service.impl.raft;

import com.lantromipis.configuration.event.PgFacadeImmediateShutdownEvent;
import com.lantromipis.configuration.event.RaftLogSyncedOnStartupEvent;
import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.orchestration.service.api.PgFacadeOrchestrator;
import com.lantromipis.orchestration.service.api.PostgresOrchestrator;
import com.lantromipis.pgfacadeprotocol.model.api.RaftRole;
import com.lantromipis.pgfacadeprotocol.server.api.RaftEventListener;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
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

    @Inject
    PostgresOrchestrator postgresOrchestrator;

    @Inject
    Event<RaftLogSyncedOnStartupEvent> raftLogSyncedOnStartupEvent;

    @Inject
    Event<PgFacadeImmediateShutdownEvent> pgFacadeImmediateShutdownEvent;

    private boolean synced = false;


    @Override
    public boolean isSyncedWithLeaderOnStartup() {
        return synced;
    }

    @Override
    public void syncedWithLeaderOrSelfIsLeaderOnStartup() {
        log.info("This PgFacade node is synced with PgFacade raft leader!");

        // Fire event. Proxy, connection pool and others should listen to it and initialize themselves
        raftLogSyncedOnStartupEvent.fire(new RaftLogSyncedOnStartupEvent());

        synced = true;
    }

    @Override
    public void selfRoleChanged(RaftRole newRaftRole) {
        try {
            if (RaftRole.LEADER.equals(newRaftRole)) {
                pgFacadeRuntimeProperties.setRaftRole(PgFacadeRaftRole.LEADER);
                log.info("This PgFacade node is leader now! Starting orchestration...");
                if (synced) {
                    // This node became leader and was synced with leader on startup. Means that this node was just promoted to leader now and previously was follower.
                    postgresOrchestrator.initializeFastWhenClusterRunning();
                } else {
                    // This node became leader but was NOT synced with leader on startup. Means that this node is alone in cluster and elected itself to manage cluster.
                    // It is this node's responsibility to start Postgres cluster.
                    postgresOrchestrator.initializeFull();
                }
                pgFacadeOrchestrator.startOrchestration();
            } else if (!PgFacadeRaftRole.FOLLOWER.equals(pgFacadeRuntimeProperties.getRaftRole())) {
                pgFacadeRuntimeProperties.setRaftRole(PgFacadeRaftRole.FOLLOWER);
                postgresOrchestrator.stopOrchestrator();
                log.info("This PgFacade node is now follower!");
            }
        } catch (Throwable t) {
            log.error("Error performing raft role change! Impossible to recover!", t);
            pgFacadeImmediateShutdownEvent.fire(new PgFacadeImmediateShutdownEvent());
        }
    }
}
