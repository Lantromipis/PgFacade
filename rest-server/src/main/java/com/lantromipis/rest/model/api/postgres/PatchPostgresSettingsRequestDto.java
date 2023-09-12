package com.lantromipis.rest.model.api.postgres;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PatchPostgresSettingsRequestDto {
    private List<PostgresSettingValueDto> settingsToPatch;
}
