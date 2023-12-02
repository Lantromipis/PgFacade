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
public interface ExternalLoadBalancerHealtcheckTemplateRestClient extends Closeable {

    @GET
    @Path("/tech/balancer-health/live")
    HealtcheckResponseDto checkLiveliness();

    @GET
    @Path("/tech/balancer-health/ready")
    HealtcheckResponseDto checkReadiness();
}
