package com.lantromipis.orchestration.orchestrator.api;

import java.util.UUID;

public interface PostgresOrchestrator {
    void initialize();

    void switchover(UUID newMasterInstanceId);
}
