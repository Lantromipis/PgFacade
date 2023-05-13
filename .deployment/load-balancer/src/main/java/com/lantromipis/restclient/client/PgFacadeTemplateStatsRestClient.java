package com.lantromipis.restclient.client;

import com.lantromipis.restclient.model.PgFacadeHttpNodesInfoResponseDto;
import com.lantromipis.restclient.model.PgFacadeSelfInfoResponseDto;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.io.Closeable;

@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Path("/api/v1/stats/pgfacade")
public interface PgFacadeTemplateStatsRestClient extends Closeable {

    @GET
    @Path("/self")
    PgFacadeSelfInfoResponseDto getSelfNodeInfo();

    @GET
    @Path("/http-nodes-info")
    PgFacadeHttpNodesInfoResponseDto getHttpNodesInfos();
}
