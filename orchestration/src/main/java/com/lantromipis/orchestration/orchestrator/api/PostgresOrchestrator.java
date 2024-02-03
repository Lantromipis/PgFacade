package com.lantromipis.orchestration.orchestrator.api;

import com.lantromipis.orchestration.exception.OrchestratorNotReadyException;
import com.lantromipis.orchestration.exception.OrchestratorOperationExecutionException;
import com.lantromipis.orchestration.model.PostgresClusterSettingsChangeResult;

import java.util.Map;

public interface PostgresOrchestrator {

    /**
     * Start orchestration of running cluster
     */
    void initializeWhenClusterRunning() throws Exception;

    /**
     * Initializes orchestrator AND Postgres cluster. Should be used only when initializing after full shutdown
     */
    void initializeWhenClusterStopped() throws Exception;

    /**
     * Stop orchestrator with an ability to reinitialize in later
     */
    void stopOrchestrator(boolean shutdownPostgres);

    PostgresClusterSettingsChangeResult changePostgresSettings(Map<String, String> newSettingNamesAndValuesMap) throws OrchestratorNotReadyException, OrchestratorOperationExecutionException;
}
