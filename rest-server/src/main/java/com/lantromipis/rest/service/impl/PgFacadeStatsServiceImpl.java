package com.lantromipis.rest.service.impl;

import com.lantromipis.connectionpool.model.stats.ConnectionPoolStats;
import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.model.PgFacadeExternalConnectionsNodeInfo;
import com.lantromipis.rest.model.stats.PgFacadeNodeExternalConnectionInfoDto;
import com.lantromipis.rest.model.stats.PgFacadeNodesInfoResponseDto;
import com.lantromipis.rest.model.stats.PgFacadeSelfInfoResponseDto;
import com.lantromipis.rest.model.stats.PgFacadeSelfPoolInfoDto;
import com.lantromipis.rest.service.api.PgFacadeStatsService;
import org.apache.commons.collections4.CollectionUtils;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@ApplicationScoped
public class PgFacadeStatsServiceImpl implements PgFacadeStatsService {

    @Inject
    ConnectionPool connectionPool;

    @Inject
    Instance<PlatformAdapter> platformAdapter;

    @Override
    public PgFacadeSelfInfoResponseDto getSelfNodeStats() {
        ConnectionPoolStats connectionPoolStats = connectionPool.getStats();

        return PgFacadeSelfInfoResponseDto
                .builder()
                .poolInfo(
                        PgFacadeSelfPoolInfoDto
                                .builder()
                                .primaryPoolConnectionLimit(connectionPoolStats.getPrimaryPoolConnectionsLimit())
                                .standbyPoolConnectionLimit(connectionPoolStats.getStandbyPoolConnectionsLimit())
                                .currentPrimaryPoolAllConnectionsCount(connectionPoolStats.getPrimaryPoolAllConnectionsCount())
                                .currentStandbyPoolAllConnectionsCount(connectionPoolStats.getStandbyPoolAllConnectionsCount())
                                .currentPrimaryPoolFreeConnectionsCount(connectionPoolStats.getPrimaryPoolFreeConnectionsCount())
                                .currentStandbyPoolFreeConnectionsCount(connectionPoolStats.getStandbyPoolFreeConnectionsCount())
                                .build()
                )
                .build();
    }

    @Override
    public PgFacadeNodesInfoResponseDto getRaftNodesInfo() {
        List<PgFacadeExternalConnectionsNodeInfo> infos = platformAdapter.get().getActivePgFacadeNodesExternalConnectionInfos();

        if (CollectionUtils.isEmpty(infos)) {
            return PgFacadeNodesInfoResponseDto
                    .builder()
                    .nodesInfo(Collections.emptyList())
                    .build();
        }

        return PgFacadeNodesInfoResponseDto
                .builder()
                .nodesInfo(
                        infos.stream()
                                .map(info -> PgFacadeNodeExternalConnectionInfoDto
                                        .builder()
                                        .ipAddress(info.getIpAddress())
                                        .httpPort(info.getHttpPort())
                                        .primaryPort(info.getPrimaryPort())
                                        .standbyPort(info.getStandbyPort())
                                        .build()
                                )
                                .collect(Collectors.toList())
                )
                .build();
    }
}
