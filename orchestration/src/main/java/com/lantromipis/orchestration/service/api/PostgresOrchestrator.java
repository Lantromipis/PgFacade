package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.*;

import java.util.Map;
import java.util.UUID;

public interface PostgresOrchestrator {

    void initialize() throws InitializationException;

    void shutdown();

    boolean switchover(UUID newPrimaryInstanceId) throws OrchestratorNotReadyException, OrchestratorNotFoundException, OrchestratorOperationExecutionException;

    void changePostgresSettings(Map<String, String> newSettingNamesAndValuesMap) throws OrchestratorNotReadyException, OrchestratorOperationExecutionException;
}
