package com.lantromipis.rest.controller;

import com.lantromipis.orchestration.service.impl.PgFacadeOrchestratorImpl;
import com.lantromipis.rest.constant.ApiConstants;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path(ApiConstants.API_V1_PREFIX + "/shutdown")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ShutdownController {

    @Inject
    PgFacadeOrchestratorImpl pgFacadeOrchestrator;

    @POST
    @Path("/raft-and-orchestration")
    public void shutdownRaft() {
        pgFacadeOrchestrator.shutdownRaftForce();
    }
}
