package com.lantromipis.orchestration.service.impl.raft;

import com.google.common.io.Resources;
import com.lantromipis.configuration.event.RaftLogSyncedOnStartupEvent;
import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.model.PgFacadeWorkMode;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.orchestration.orchestrator.api.LoadBalancerOrchestrator;
import com.lantromipis.orchestration.orchestrator.api.PgFacadeOrchestrator;
import com.lantromipis.orchestration.orchestrator.api.PostgresOrchestrator;
import com.lantromipis.pgfacadeprotocol.model.api.RaftRole;
import com.lantromipis.pgfacadeprotocol.server.api.RaftEventListener;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.nio.charset.StandardCharsets;

@Slf4j
@ApplicationScoped
public class RaftEventListenerImpl implements RaftEventListener {

    @Inject
    PgFacadeOrchestrator pgFacadeOrchestrator;

    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    @Inject
    PostgresOrchestrator postgresOrchestrator;

    @Inject
    LoadBalancerOrchestrator loadBalancerOrchestrator;

    @Inject
    Event<RaftLogSyncedOnStartupEvent> raftLogSyncedOnStartupEvent;

    private boolean synced = false;

    @Override
    public boolean isSyncedWithLeaderOnStartup() {
        return synced;
    }

    @Override
    public void syncedWithLeaderOrSelfIsLeaderOnStartup() {
        // if in recovery, no need to fire any events
        if (PgFacadeWorkMode.RECOVERY.equals(pgFacadeRuntimeProperties.getWorkMode())) {
            return;
        }

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
                    postgresOrchestrator.initializeWhenClusterRunning();
                } else {
                    // This node became leader but was NOT synced with leader on startup. Means that this node is alone in cluster and elected itself to manage cluster.
                    // It is this node's responsibility to start Postgres cluster.
                    postgresOrchestrator.initializeWhenClusterStopped();
                }
                pgFacadeOrchestrator.startOrchestration();
                loadBalancerOrchestrator.startOrchestration();
            } else if (!PgFacadeRaftRole.FOLLOWER.equals(pgFacadeRuntimeProperties.getRaftRole())) {
                pgFacadeRuntimeProperties.setRaftRole(PgFacadeRaftRole.FOLLOWER);
                postgresOrchestrator.stopOrchestrator(false);
                pgFacadeOrchestrator.stopOrchestration();
                loadBalancerOrchestrator.stopOrchestration();
                log.info("This PgFacade node is now follower!");
            } else {
                log.info("This PgFacade node is already leader. No actions required.");
            }
        } catch (Throwable t) {
            log.error("Error performing raft role change! Impossible to recover!", t);
            enterRecoveryMode();
        }
    }

    private void enterRecoveryMode() {
        pgFacadeRuntimeProperties.setWorkMode(PgFacadeWorkMode.RECOVERY);
        try {
            URL url = Resources.getResource("logger/banners/EnteredRecoveryModeBanner.txt");
            String text = Resources.toString(url, StandardCharsets.UTF_8);

            log.info(text);

        } catch (Exception e) {
            log.info("ENTERED RECOVERY MODE!", e);
        }
    }
}
