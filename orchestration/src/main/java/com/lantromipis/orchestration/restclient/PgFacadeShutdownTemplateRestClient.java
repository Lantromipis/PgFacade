package com.lantromipis.orchestration.restclient;

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
    @Path("/shutdown/raft-and-orchestration")
    void shutdownRaft();
}
