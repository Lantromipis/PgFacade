package com.lantromipis.rest.service.api;

import com.lantromipis.rest.model.api.postgres.PatchPostgresSettingsRequestDto;
import com.lantromipis.rest.model.api.postgres.PatchPostgresSettingsResponseDto;
import com.lantromipis.rest.model.api.postgres.PostgresSettingsResponseDto;

public interface PostgresManagementService {
    PostgresSettingsResponseDto getCurrentSettings();

    PatchPostgresSettingsResponseDto patchSettings(PatchPostgresSettingsRequestDto requestDto);
}
