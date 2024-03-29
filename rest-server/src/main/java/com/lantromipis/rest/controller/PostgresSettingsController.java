package com.lantromipis.rest.controller;

import com.lantromipis.rest.constant.ApiConstants;
import com.lantromipis.rest.filter.namebinding.CheckNotInRecoveryState;
import com.lantromipis.rest.model.api.postgres.PatchPostgresSettingsRequestDto;
import com.lantromipis.rest.model.api.postgres.PatchPostgresSettingsResponseDto;
import com.lantromipis.rest.model.api.postgres.PostgresSettingsResponseDto;
import com.lantromipis.rest.service.api.PostgresManagementService;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

@CheckNotInRecoveryState
@Path(ApiConstants.API_V1_PREFIX + "/postgresManagement/settings")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PostgresSettingsController {

    @Inject
    PostgresManagementService postgresManagementService;

    @GET
    public PostgresSettingsResponseDto getPostgresSettings() {
        return postgresManagementService.getCurrentSettings();
    }

    @PATCH
    public PatchPostgresSettingsResponseDto patchPostgresSettings(PatchPostgresSettingsRequestDto requestDto) {
        return postgresManagementService.patchSettings(requestDto);
    }
}
