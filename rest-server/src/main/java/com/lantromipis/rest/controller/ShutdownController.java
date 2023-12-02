package com.lantromipis.rest.controller;

import com.lantromipis.orchestration.service.api.PgFacadeOrchestrator;
import com.lantromipis.rest.constant.ApiConstants;
import com.lantromipis.rest.filter.namebinding.CheckNotInRecoveryState;
import com.lantromipis.rest.model.api.shutdown.ForceShutdownRequestDto;
import com.lantromipis.rest.model.api.shutdown.ShutdownMessageResponseDto;
import com.lantromipis.rest.model.api.shutdown.ShutdownRaftAndOrchestrationRequestDto;
import com.lantromipis.rest.model.api.shutdown.SoftShutdownRequestDto;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@CheckNotInRecoveryState
@Path(ApiConstants.API_V1_PREFIX + "/shutdown")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ShutdownController {

    @Inject
    PgFacadeOrchestrator pgFacadeOrchestrator;

    @POST
    @Path("/raft-and-orchestration")
    public Response shutdownRaftAndOrchestration(ShutdownRaftAndOrchestrationRequestDto requestDto) {
        log.info("Received HTTP request to shutdown Raft server and any Orchestration.");

        if (pgFacadeOrchestrator.shutdownClusterRaftAndOrchestration(requestDto.isSuspend())) {
            log.info("Raft server and any Orchestration stopped due to HTTP request.");
            return Response.ok(
                            ShutdownMessageResponseDto
                                    .builder()
                                    .message("Raft and orchestration was shut down and container was suspended. Proxy is still working.")
                                    .build()
                    )
                    .build();
        } else {
            log.info("Failed to stop Raft server and any Orchestration as was requested by HTTP request.");
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(
                            ShutdownMessageResponseDto
                                    .builder()
                                    .message("Some error occurred while shutting down raft and orchestration. Examine logs and retry.")
                                    .build()
                    )
                    .build();
        }
    }

    @POST
    @Path("/soft")
    public Response shutdownProxySoftAndPgFacade(SoftShutdownRequestDto requestDto) {
        log.info("Received HTTP request to soft shutdown PgFacade.");

        if (requestDto.getMaxClientsAwaitPeriodSeconds() == 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(
                            ShutdownMessageResponseDto
                                    .builder()
                                    .message("Specify 'maxClientsAwaitPeriodSeconds' in request (greater than zero).")
                                    .build()
                    )
                    .build();
        }

        boolean success = pgFacadeOrchestrator.shutdownClusterFull(
                false,
                requestDto.isShutdownPostgres(),
                requestDto.isShutdownLoadBalancer(),
                requestDto.getMaxClientsAwaitPeriodSeconds()
        );

        if (success) {
            log.info("Soft shutdown initiated due to HTTP request.");
            return Response.ok(
                            ShutdownMessageResponseDto
                                    .builder()
                                    .message("Proxy not accepting new connections. PgFacade will shutdown automatically when all clients will disconnect or after " + requestDto.getMaxClientsAwaitPeriodSeconds() + " ms from now.")
                                    .build()
                    )
                    .build();
        } else {
            log.info("Failed to complete soft shutdown as was requested by HTTP request.");
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(
                            ShutdownMessageResponseDto
                                    .builder()
                                    .message("Some error occurred while shutting down cluster. Examine logs and retry.")
                                    .build()
                    )
                    .build();
        }
    }

    @POST
    @Path("/force")
    public Response shutdownProxyForceAndPgFacade(ForceShutdownRequestDto requestDto) {
        log.info("Received HTTP request to force shutdown PgFacade.");

        boolean success = pgFacadeOrchestrator.shutdownClusterFull(
                true,
                requestDto.isShutdownPostgres(),
                requestDto.isShutdownLoadBalancer(),
                0
        );

        if (success) {
            log.info("Force shutdown initiated due to HTTP request.");
            return Response.ok(
                            ShutdownMessageResponseDto
                                    .builder()
                                    .message("PgFacade will be shutdown shortly...")
                                    .build()
                    )
                    .build();
        } else {
            log.info("Failed to complete force shutdown as was requested by HTTP request.");
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(
                            ShutdownMessageResponseDto
                                    .builder()
                                    .message("Some error occurred while shutting down cluster. Examine logs and retry.")
                                    .build()
                    )
                    .build();
        }
    }
}
