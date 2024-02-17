package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.model.PostgresCombinedInstanceInfo;
import com.lantromipis.orchestration.model.StandbyElectionForPromotionResult;

import java.util.List;

public interface PostgresStandbyOrchestrationService {
    List<PostgresCombinedInstanceInfo> startStoppedStandbys();

    void checkStandbyCountAndLiveliness();

    StandbyElectionForPromotionResult selectStandbyForPromotion();
}
