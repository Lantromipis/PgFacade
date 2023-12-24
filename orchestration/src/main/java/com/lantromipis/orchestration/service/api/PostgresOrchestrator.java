package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.OrchestratorNotFoundException;
import com.lantromipis.orchestration.exception.OrchestratorNotReadyException;
import com.lantromipis.orchestration.exception.OrchestratorOperationExecutionException;

import java.util.Map;
import java.util.UUID;

public interface PostgresOrchestrator {

    /**
     * Start orchestration of running cluster
     */
    void initializeFastWhenClusterRunning() throws Exception;

    /**
     * Initializes orchestrator AND Postgres cluster. Should be used only when initializing after full shutdown
     */
    void initializeFull() throws Exception;

    /**
     * Stop orchestrator with an ability to reinitialize in later
     */
    void stopOrchestrator(boolean shutdownPostgres);

    boolean switchover(UUID newPrimaryInstanceId) throws OrchestratorNotReadyException, OrchestratorNotFoundException, OrchestratorOperationExecutionException;

    void changePostgresSettings(Map<String, String> newSettingNamesAndValuesMap) throws OrchestratorNotReadyException, OrchestratorOperationExecutionException;
}
