package com.lantromipis.rest.controller;

import com.lantromipis.rest.constant.ApiConstants;
import com.lantromipis.rest.model.stats.PgFacadeHttpNodesInfoResponseDto;
import com.lantromipis.rest.model.stats.PgFacadeSelfInfoResponseDto;
import com.lantromipis.rest.service.impl.PgFacadeStatsServiceImpl;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

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
