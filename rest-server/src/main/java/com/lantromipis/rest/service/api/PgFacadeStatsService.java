package com.lantromipis.rest.service.api;

import com.lantromipis.rest.model.api.stats.PgFacadeHttpNodesInfoResponseDto;
import com.lantromipis.rest.model.api.stats.PgFacadeSelfInfoResponseDto;

public interface PgFacadeStatsService {
    PgFacadeSelfInfoResponseDto getSelfNodeStats();

    PgFacadeHttpNodesInfoResponseDto getHttpNodesInfo();
}
