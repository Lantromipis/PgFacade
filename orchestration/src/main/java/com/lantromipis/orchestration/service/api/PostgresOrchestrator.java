package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.OrchestratorNotReadyException;
import com.lantromipis.orchestration.exception.PostgresConfigurationChangeException;

import java.util.Map;
import java.util.UUID;

public interface PostgresOrchestrator {
    boolean initialize();

    void shutdown();

    boolean switchover(UUID newMasterInstanceId) throws OrchestratorNotReadyException;

    void changePostgresSettings(Map<String, String> newSettingNamesAndValuesMap) throws PostgresConfigurationChangeException, OrchestratorNotReadyException;
}
