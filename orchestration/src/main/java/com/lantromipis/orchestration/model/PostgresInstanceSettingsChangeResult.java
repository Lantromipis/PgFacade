package com.lantromipis.orchestration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.Set;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostgresInstanceSettingsChangeResult {
    private Status status;
    // all below filled in case of SUCCESS status
    private boolean restartRequired;

    // all below filed in case of SETTINGS_INVALID status
    private Map<String, String> settingNameToValidationError;

    // all below filed in case of SQL_ERROR status
    private String sqlErrorMessage;
    private boolean rollbackWasRequired;
    private Set<String> notRollbackedSettings;

    public enum Status {
        SUCCESS,
        SETTINGS_INVALID,
        INTERNAL_ERROR,
        SQL_ERROR
    }
}
