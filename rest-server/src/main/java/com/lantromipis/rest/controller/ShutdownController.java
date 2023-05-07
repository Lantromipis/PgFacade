package com.lantromipis.rest.controller;

import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.orchestration.service.impl.PgFacadeOrchestratorImpl;
import com.lantromipis.proxy.service.api.PgProxyService;
import com.lantromipis.rest.constant.ApiConstants;
import com.lantromipis.rest.model.shutdown.ProxySoftShutdownRequestDto;
import com.lantromipis.rest.model.shutdown.ShutdownMessageResponseDto;
import io.quarkus.runtime.Quarkus;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.time.Duration;

@Slf4j
@Path(ApiConstants.API_V1_PREFIX + "/shutdown")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ShutdownController {

    @Inject
    PgFacadeOrchestratorImpl pgFacadeOrchestrator;

    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    @Inject
    PgProxyService proxyService;

    @Inject
    ManagedExecutor managedExecutor;

    @POST
    @Path("/raft-and-orchestration")
    public ShutdownMessageResponseDto shutdownRaft() {
        PgFacadeRaftRole raftRole = pgFacadeRuntimeProperties.getRaftRole();
        if (pgFacadeOrchestrator.shutdownRaftAndOrchestartionForce()) {
            if (PgFacadeRaftRole.LEADER.equals(raftRole)) {
                return ShutdownMessageResponseDto
                        .builder()
                        .message("Successful shutdown of all Raft Servers in cluster. Orchestration is disabled in all cluster!")
                        .build();
            } else {
                return ShutdownMessageResponseDto
                        .builder()
                        .message("Successful shutdown of local Raft Server. Orchestration for this nodes is disabled.")
                        .build();
            }
        } else {
            return ShutdownMessageResponseDto
                    .builder()
                    .message("Some error occurred while preforming shutdown. Examine logs.")
                    .build();
        }
    }

    @POST
    @Path("/proxy-soft-and-then-pgfacade")
    public ShutdownMessageResponseDto shutdownProxySoftAndPgFacade(ProxySoftShutdownRequestDto requestDto) {
        if (!PgFacadeRaftRole.RAFT_DISABLED.equals(pgFacadeRuntimeProperties.getRaftRole())) {
            return ShutdownMessageResponseDto
                    .builder()
                    .message("Raft is still enabled, but must be disabled prior to calling this method. Call POST '/api/v1/shutdown/raft-and-orchestration' first to disable Raft.")
                    .build();
        }

        if (requestDto.getMaxClientsAwaitPeriodMs() == 0) {
            return ShutdownMessageResponseDto
                    .builder()
                    .message("Specify 'maxClientsAwaitPeriodMs' in request (greater than zero).")
                    .build();
        }

        managedExecutor.runAsync(() -> {
                    proxyService.shutdown(true, Duration.ofMillis(requestDto.getMaxClientsAwaitPeriodMs()));
                    log.info("All clients disconnected. Shutting down PgFacade...");
                    Quarkus.asyncExit(0);
                }
        );

        return ShutdownMessageResponseDto
                .builder()
                .message("Proxy not accepting new connections. PgFacade will shutdown automatically when all clients will disconnect or after " + requestDto.getMaxClientsAwaitPeriodMs() + " ms from now.")
                .build();
    }

    @POST
    @Path("/pgfacade-force")
    public ShutdownMessageResponseDto shutdownProxyForceAndPgFacade() {
        managedExecutor.runAsync(() -> {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        // ignored
                    } finally {
                        Quarkus.asyncExit(0);
                    }
                }
        );

        return ShutdownMessageResponseDto
                .builder()
                .message("PgFacade will be shutdown shortly..")
                .build();
    }
}
