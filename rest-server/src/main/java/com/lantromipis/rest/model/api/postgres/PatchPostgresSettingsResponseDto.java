package com.lantromipis.rest.model.api.postgres;

import com.fasterxml.jackson.annotation.JsonValue;
import lombok.*;

import java.util.Map;
import java.util.Set;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PatchPostgresSettingsResponseDto {
    private Status status;

    // in case VALIDATION_ERROR status
    private Map<String, String> settingNameToValidationError;

    // in case SETTING_CHANGE_ERROR status
    private Set<String> settingNamesForWhichRollbackFailed;
    private String settingChangeErrorMessage;

    @Getter
    @RequiredArgsConstructor
    public enum Status {
        SUCCESS("Success"),
        VALIDATION_ERROR("Validation error"),
        SETTING_CHANGE_ERROR("Setting change error"),
        NOT_READY_ERROR("Not ready"),
        CLUSTER_MODIFICATION_IN_PROGRESS_ERROR("Cluster modification already in progress"),
        NO_AVAILABLE_STANDBY_ERROR("No available standby"),
        NO_PRIMARY_ERROR("No primary"),
        RESTART_AFTER_CHANGE_ERROR("Restart after change error"),
        UNKNOWN_ERROR("Unknown error");

        @JsonValue
        private final String jsonValue;
    }
}
