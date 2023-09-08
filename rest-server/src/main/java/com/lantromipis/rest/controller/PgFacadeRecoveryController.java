package com.lantromipis.rest.controller;

import com.lantromipis.rest.constant.ApiConstants;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path(ApiConstants.API_V1_PREFIX + "/recovery")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PgFacadeRecoveryController {
    @POST
    @Path("/restore-postgres-from-backup")
    public void RestorePostgresFromBackup() {

    }
}
