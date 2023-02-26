package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.PostgresConfigurationChangeException;

import java.util.Map;
import java.util.UUID;

public interface PostgresOrchestrator {
    void initialize();

    void shutdown();

    void switchover(UUID newMasterInstanceId);

    void changePostgresSettings(Map<String, String> newSettingNamesAndValuesMap) throws PostgresConfigurationChangeException;
}
