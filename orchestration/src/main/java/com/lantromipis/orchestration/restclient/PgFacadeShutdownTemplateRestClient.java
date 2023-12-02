package com.lantromipis.orchestration.restclient;

import com.lantromipis.orchestration.restclient.model.ForceShutdownRequestDto;
import com.lantromipis.orchestration.restclient.model.ShutdownRaftAndOrchestrationRequestDto;
import com.lantromipis.orchestration.restclient.model.SoftShutdownRequestDto;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.io.Closeable;

@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Path("")
public interface PgFacadeShutdownTemplateRestClient extends Closeable {

    @POST
    @Path("/api/v1/shutdown/raft-and-orchestration")
    void shutdownRaftAndOrchestration(ShutdownRaftAndOrchestrationRequestDto requestDto);

    @POST
    @Path("/api/v1/shutdown/soft")
    void shutdownSoft(SoftShutdownRequestDto requestDto);

    @POST
    @Path("/api/v1/shutdown/force")
    void shutdownForce(ForceShutdownRequestDto requestDto);
}
