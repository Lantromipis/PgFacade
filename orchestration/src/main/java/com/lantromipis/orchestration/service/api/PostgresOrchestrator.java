package com.lantromipis.orchestration.service.api;

import java.util.UUID;

public interface PostgresOrchestrator {
    void initialize();

    void switchover(UUID newMasterInstanceId);
}
