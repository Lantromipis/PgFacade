package com.lantromipis.rest.service.api;

import com.lantromipis.rest.model.stats.PgFacadeNodesInfoResponseDto;
import com.lantromipis.rest.model.stats.PgFacadeSelfInfoResponseDto;

public interface PgFacadeStatsService {
    PgFacadeSelfInfoResponseDto getSelfNodeStats();

    PgFacadeNodesInfoResponseDto getRaftNodesInfo();
}
