package com.lantromipis.rest.service.impl;

import com.lantromipis.connectionpool.model.stats.ConnectionPoolStats;
import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.orchestration.adapter.api.PgFacadePlatformAdapter;
import com.lantromipis.orchestration.model.PgFacadeNodeExternalConnectionsInfo;
import com.lantromipis.orchestration.model.PgFacadeNodeHttpConnectionsInfo;
import com.lantromipis.rest.model.api.stats.*;
import com.lantromipis.rest.service.api.PgFacadeStatsService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@ApplicationScoped
public class PgFacadeStatsServiceImpl implements PgFacadeStatsService {

    @Inject
    ConnectionPool connectionPool;

    @Inject
    Instance<PgFacadePlatformAdapter> platformAdapter;

    @Override
    public PgFacadeSelfInfoResponseDto getSelfNodeStats() {
        ConnectionPoolStats connectionPoolStats = connectionPool.getStats();
        PgFacadeNodeExternalConnectionsInfo selfExternalConnectionInfo = platformAdapter.get().getSelfExternalConnectionInfo();

        return PgFacadeSelfInfoResponseDto
                .builder()
                .poolInfo(
                        PgFacadePoolInfoDto
                                .builder()
                                .primaryPoolConnectionLimit(connectionPoolStats.getPrimaryPoolConnectionsLimit())
                                .standbyPoolConnectionLimit(connectionPoolStats.getStandbyPoolConnectionsLimit())
                                .currentPrimaryPoolAllConnectionsCount(connectionPoolStats.getPrimaryPoolAllConnectionsCount())
                                .currentStandbyPoolAllConnectionsCount(connectionPoolStats.getStandbyPoolAllConnectionsCount())
                                .currentPrimaryPoolFreeConnectionsCount(connectionPoolStats.getPrimaryPoolFreeConnectionsCount())
                                .currentStandbyPoolFreeConnectionsCount(connectionPoolStats.getStandbyPoolFreeConnectionsCount())
                                .build()
                )
                .externalConnectionInfo(
                        PgFacadeSelfExternalConnectionInfoDto
                                .builder()
                                .address(selfExternalConnectionInfo.getAddress())
                                .httpPort(selfExternalConnectionInfo.getHttpPort())
                                .standbyPort(selfExternalConnectionInfo.getStandbyPort())
                                .primaryPort(selfExternalConnectionInfo.getPrimaryPort())
                                .build()
                )
                .build();
    }

    @Override
    public PgFacadeHttpNodesInfoResponseDto getHttpNodesInfo() {
        List<PgFacadeNodeHttpConnectionsInfo> infos = platformAdapter.get().getActivePgFacadeHttpNodesInfos();

        if (CollectionUtils.isEmpty(infos)) {
            return PgFacadeHttpNodesInfoResponseDto
                    .builder()
                    .httpNodesInfo(Collections.emptyList())
                    .build();
        }

        return PgFacadeHttpNodesInfoResponseDto
                .builder()
                .httpNodesInfo(
                        infos.stream()
                                .map(info -> PgFacadeNodeHttpConnectionInfo
                                        .builder()
                                        .address(info.getAddress())
                                        .httpPort(info.getPort())
                                        .build()
                                )
                                .collect(Collectors.toList())
                )
                .build();
    }
}
