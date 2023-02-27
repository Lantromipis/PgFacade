package com.lantromipis.rest.controller;

import com.lantromipis.rest.constant.ApiConstants;
import com.lantromipis.rest.model.postgres.PatchPostgresSettingsRequestDto;
import com.lantromipis.rest.model.postgres.PostgresSettingsResponseDto;
import com.lantromipis.rest.service.api.PostgresManagementService;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path(ApiConstants.API_PREFIX + "/postgresManagement")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PostgresManagementController {

    @Inject
    PostgresManagementService postgresManagementService;

    @GET
    @Path("/settings")
    public PostgresSettingsResponseDto getPostgresSettings() {
        return postgresManagementService.getCurrentSettings();
    }

    @PATCH
    @Path("/settings")
    public PostgresSettingsResponseDto patchPostgresSettings(PatchPostgresSettingsRequestDto requestDto) {
        return postgresManagementService.patchSettings(requestDto);
    }
}
