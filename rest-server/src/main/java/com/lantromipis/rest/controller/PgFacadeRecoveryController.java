package com.lantromipis.rest.controller;

import com.lantromipis.rest.constant.ApiConstants;
import com.lantromipis.rest.filter.namebinding.CheckInRecoveryState;
import com.lantromipis.rest.model.api.recovery.PostgresRestoreRequestDto;
import com.lantromipis.rest.model.internal.PostgresRestoreSettings;
import com.lantromipis.rest.service.api.PostgresRecoveryService;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

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
