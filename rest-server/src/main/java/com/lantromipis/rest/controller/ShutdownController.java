package com.lantromipis.rest.controller;

import com.lantromipis.orchestration.service.impl.PgFacadeOrchestratorImpl;
import com.lantromipis.rest.constant.ApiConstants;
import com.lantromipis.rest.model.shutdown.ForceShutdownRequestDto;
import com.lantromipis.rest.model.shutdown.ShutdownMessageResponseDto;
import com.lantromipis.rest.model.shutdown.SoftShutdownRequestDto;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Slf4j
@Path(ApiConstants.API_V1_PREFIX + "/shutdown")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ShutdownController {

    @Inject
    PgFacadeOrchestratorImpl pgFacadeOrchestrator;

    @POST
    @Path("/soft")
    public Response shutdownProxySoftAndPgFacade(SoftShutdownRequestDto requestDto) {
        if (requestDto.getMaxClientsAwaitPeriodMs() == 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(
                            ShutdownMessageResponseDto
                                    .builder()
                                    .message("Specify 'maxClientsAwaitPeriodMs' in request (greater than zero).")
                                    .build()
                    )
                    .build();
        }

        boolean success = pgFacadeOrchestrator.shutdownCluster(false, requestDto.isShutdownPostgres(), requestDto.getMaxClientsAwaitPeriodMs());

        if (success) {
            return Response.ok(
                            ShutdownMessageResponseDto
                                    .builder()
                                    .message("Proxy not accepting new connections. PgFacade will shutdown automatically when all clients will disconnect or after " + requestDto.getMaxClientsAwaitPeriodMs() + " ms from now.")
                                    .build()
                    )
                    .build();
        } else {
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
        boolean success = pgFacadeOrchestrator.shutdownCluster(true, requestDto.isShutdownPostgres(), 0);

        if (success) {
            return Response.ok(
                            ShutdownMessageResponseDto
                                    .builder()
                                    .message("PgFacade will be shutdown shortly...")
                                    .build()
                    )
                    .build();
        } else {
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
