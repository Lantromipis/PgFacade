package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.model.PostgresCombinedInstanceInfo;
import com.lantromipis.orchestration.model.SelectedForPromotionStandby;

import java.util.List;

public interface PostgresStandbyOrchestrationService {
    List<PostgresCombinedInstanceInfo> startStoppedStandbys();

    void checkStandbyCountAndLiveliness();

    SelectedForPromotionStandby selectStandbyForPromotion();
}
