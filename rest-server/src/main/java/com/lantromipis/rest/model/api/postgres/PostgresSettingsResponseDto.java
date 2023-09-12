package com.lantromipis.rest.model.api.postgres;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PostgresSettingsResponseDto {
    private List<String> importantNotes;
    private List<PostgresSettingContextDescription> settingContextDescriptions;
    private List<PostgresSettingDescriptionDto> currentSettings;
}