package com.lantromipis.orchestration.restclient;

import com.lantromipis.orchestration.restclient.model.HealtcheckResponseDto;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.Closeable;

@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Path("")
public interface HealtcheckTemplateRestClient extends Closeable {

    @GET
    @Path("/q/health/live")
    HealtcheckResponseDto checkLiveliness();

    @GET
    @Path("/q/health/ready")
    HealtcheckResponseDto checkReadiness();
}
