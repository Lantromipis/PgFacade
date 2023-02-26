package com.lantromipis.rest.service.api;

import com.lantromipis.rest.model.postgres.PatchPostgresSettingsRequestDto;
import com.lantromipis.rest.model.postgres.PostgresSettingsResponseDto;

public interface PostgresManagementService {
    PostgresSettingsResponseDto getCurrentSettings();

    PostgresSettingsResponseDto patchSettings(PatchPostgresSettingsRequestDto requestDto);
}
