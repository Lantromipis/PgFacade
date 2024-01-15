package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.NoPrimaryException;
import com.lantromipis.orchestration.model.PostgresCombinedInstanceInfo;

public interface PostgresPrimaryOrchestrationService {
    PostgresCombinedInstanceInfo startStoppedExistingPrimary() throws NoPrimaryException;
}
