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
public interface ExternalLoadBalancerHealtcheckTemplateRestClient extends Closeable {

    @GET
    @Path("/tech/balancer-health/live")
    HealtcheckResponseDto checkLiveliness();

    @GET
    @Path("/tech/balancer-health/ready")
    HealtcheckResponseDto checkReadiness();
}
