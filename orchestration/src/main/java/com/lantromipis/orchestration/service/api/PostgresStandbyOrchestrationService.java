package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.model.PostgresCombinedInstanceInfo;

import java.util.List;

public interface PostgresStandbyOrchestrationService {
    List<PostgresCombinedInstanceInfo> startStoppedStandbys();
}
