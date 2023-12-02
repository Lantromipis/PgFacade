package com.lantromipis.rest.controller;

import com.lantromipis.rest.constant.ApiConstants;
import com.lantromipis.rest.filter.namebinding.CheckNotInRecoveryState;
import com.lantromipis.rest.model.api.stats.PgFacadeHttpNodesInfoResponseDto;
import com.lantromipis.rest.model.api.stats.PgFacadeSelfInfoResponseDto;
import com.lantromipis.rest.service.impl.PgFacadeStatsServiceImpl;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@CheckNotInRecoveryState
@Path(ApiConstants.API_V1_PREFIX + "/stats/pgfacade")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PgFacadeStatsController {

    @Inject
    PgFacadeStatsServiceImpl pgFacadeStatsService;

    @GET
    @Path("/self")
    public PgFacadeSelfInfoResponseDto getSelfNodeInfo() {
        return pgFacadeStatsService.getSelfNodeStats();
    }

    @GET
    @Path("/http-nodes-info")
    public PgFacadeHttpNodesInfoResponseDto getRaftNodesInfo() {
        return pgFacadeStatsService.getHttpNodesInfo();
    }
}
