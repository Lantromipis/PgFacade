package com.lantromipis.rest.controller;

import com.lantromipis.rest.constant.ApiConstants;
import com.lantromipis.rest.filter.namebinding.CheckInRecoveryState;
import com.lantromipis.rest.model.api.recovery.PostgresRestoreRequestDto;
import com.lantromipis.rest.model.internal.PostgresRestoreSettings;
import com.lantromipis.rest.service.api.PostgresRecoveryService;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@CheckInRecoveryState
@Path(ApiConstants.API_V1_PREFIX + "/recovery")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PgFacadeRecoveryController {

    @Inject
    PostgresRecoveryService postgresRecoveryService;

    @POST
    @Path("/restore-postgres/to-latest")
    public void RestorePostgresFromBackupToLatest(PostgresRestoreRequestDto requestDto) {
        postgresRecoveryService.startRecoveryFromBackup(
                PostgresRestoreSettings
                        .builder()
                        .saveAsNewPrimary(requestDto.isSaveRestoredInstanceAsNewPrimary())
                        .build()
        );
    }
}
