package com.lantromipis.rest.service.api;

import com.lantromipis.rest.model.stats.PgFacadeHttpNodesInfoResponseDto;
import com.lantromipis.rest.model.stats.PgFacadeSelfInfoResponseDto;

public interface PgFacadeStatsService {
    PgFacadeSelfInfoResponseDto getSelfNodeStats();

    PgFacadeHttpNodesInfoResponseDto getHttpNodesInfo();
}
