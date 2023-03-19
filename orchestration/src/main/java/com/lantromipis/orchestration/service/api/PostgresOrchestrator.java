package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.OrchestratorNotFoundException;
import com.lantromipis.orchestration.exception.OrchestratorNotReadyException;
import com.lantromipis.orchestration.exception.OrchestratorOperationExecutionException;
import com.lantromipis.orchestration.exception.PostgresConfigurationChangeException;

import java.util.Map;
import java.util.UUID;

public interface PostgresOrchestrator {
    boolean initialize();

    void shutdown();

    boolean switchover(UUID newPrimaryInstanceId) throws OrchestratorNotReadyException, OrchestratorNotFoundException, OrchestratorOperationExecutionException;

    void changePostgresSettings(Map<String, String> newSettingNamesAndValuesMap) throws OrchestratorNotReadyException, OrchestratorOperationExecutionException;
}
