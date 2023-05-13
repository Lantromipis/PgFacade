package com.lantromipis.client;

import com.lantromipis.client.model.ForceShutdownRequestDto;
import com.lantromipis.client.model.ShutdownRaftAndOrchestrationRequestDto;
import com.lantromipis.client.model.SoftShutdownRequestDto;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.Closeable;

@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Path("")
public interface PgFacadeShutdownTemplateRestClient extends Closeable {

    @POST
    @Path("/api/v1/shutdown/soft")
    void shutdownSoft(SoftShutdownRequestDto requestDto);

    @POST
    @Path("/api/v1/shutdown/force")
    void shutdownForce(ForceShutdownRequestDto requestDto);

    @POST
    @Path("/api/v1/shutdown/raft-and-orchestration")
    void shutdownRaftAndOrchestration(ShutdownRaftAndOrchestrationRequestDto requestDto);
}
