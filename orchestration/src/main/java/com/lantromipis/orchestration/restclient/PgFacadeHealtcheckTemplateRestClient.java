package com.lantromipis.orchestration.restclient;

import com.lantromipis.orchestration.restclient.model.HealtcheckResponseDto;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.io.Closeable;

@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Path("")
public interface PgFacadeHealtcheckTemplateRestClient extends Closeable {

    @GET
    @Path("/q/health/live")
    HealtcheckResponseDto checkLiveliness();

    @GET
    @Path("/q/health/ready")
    HealtcheckResponseDto checkReadiness();
}
