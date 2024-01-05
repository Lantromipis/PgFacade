package com.lantromipis.orchestration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostgresSettingsValidationResult {
    private boolean restartRequired;
    private boolean settingsValid;
    private Map<String, String> settingNameToError;
}
