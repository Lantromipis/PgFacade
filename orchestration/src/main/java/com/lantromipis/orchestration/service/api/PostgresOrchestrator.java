package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.*;

import java.util.Map;
import java.util.UUID;

public interface PostgresOrchestrator {

    /**
     * Start orchestration of running cluster
     */
    void initializeFastWhenClusterRunning();

    /**
     * Initializes orchestrator AND Postgres cluster. Should be used only when initializing after full shutdown
     *
     * @throws NoPrimaryException      if there is no primary
     * @throws InitializationException if initialization failed
     */
    void initializeFull() throws NoPrimaryException, InitializationException;

    /**
     * Stop orchestrator with an ability to reinitialize in later
     */
    void stopOrchestrator(boolean shutdownPostgres);

    boolean switchover(UUID newPrimaryInstanceId) throws OrchestratorNotReadyException, OrchestratorNotFoundException, OrchestratorOperationExecutionException;

    void changePostgresSettings(Map<String, String> newSettingNamesAndValuesMap) throws OrchestratorNotReadyException, OrchestratorOperationExecutionException;
}
