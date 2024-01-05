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
public class PostgresClusterSettingsChangeResult {
    private Status status;

    // in case VALIDATION_ERROR status
    private Map<String, String> settingNameToValidationError;

    // in case SETTING_CHANGE_ERROR status
    private Set<String> settingNamesForWhichRollbackFailed;
    private String settingChangeErrorMessage;

    public enum Status {
        SUCCESS,
        VALIDATION_ERROR,
        SETTING_CHANGE_ERROR,
        NOT_READY_ERROR,
        CLUSTER_MODIFICATION_IN_PROGRESS_ERROR,
        NO_AVAILABLE_STANDBY_ERROR,
        NO_PRIMARY_ERROR,
        RESTART_AFTER_CHANGE_ERROR,
        UNKNOWN_ERROR
    }
}
