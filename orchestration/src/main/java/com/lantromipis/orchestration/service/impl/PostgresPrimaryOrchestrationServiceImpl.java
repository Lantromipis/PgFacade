package com.lantromipis.orchestration.service.impl;

import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.NoPrimaryException;
import com.lantromipis.orchestration.model.PostgresAdapterInstanceInfo;
import com.lantromipis.orchestration.model.PostgresCombinedInstanceInfo;
import com.lantromipis.orchestration.model.raft.PostgresPersistedInstanceInfo;
import com.lantromipis.orchestration.service.api.PostgresPrimaryOrchestrationService;
import com.lantromipis.orchestration.util.OrchestratorUtils;
import com.lantromipis.orchestration.util.RaftFunctionalityCombinator;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ApplicationScoped
public class PostgresPrimaryOrchestrationServiceImpl implements PostgresPrimaryOrchestrationService {

    @Inject
    RaftFunctionalityCombinator raftFunctionalityCombinator;

    @Inject
    Instance<PlatformAdapter> platformAdapter;

    @Inject
    OrchestratorUtils orchestratorUtils;

    @Override
    public PostgresCombinedInstanceInfo startStoppedExistingPrimary() throws NoPrimaryException {
        PostgresPersistedInstanceInfo primaryPersistedInstanceInfo = raftFunctionalityCombinator.getPostgresNodeInfos()
                .stream()
                .filter(PostgresPersistedInstanceInfo::isPrimary)
                .findFirst()
                .orElse(null);

        PostgresAdapterInstanceInfo adapterInstanceInfo;

        if (primaryPersistedInstanceInfo == null) {
            throw new NoPrimaryException("Postgres primary NOT found! Con not start orchestration!");
        }

        try {
            adapterInstanceInfo = platformAdapter.get().getPostgresInstanceInfo(primaryPersistedInstanceInfo.getAdapterIdentifier());
        } catch (Exception e) {
            throw new NoPrimaryException("Failed to retrieve known Postgres primary information from platform adapter! Con not start orchestration!", e);
        }

        if (adapterInstanceInfo.isActive()) {
            log.info("Found active Postgres primary!");
        } else {
            log.info("Found non-active Postgres primary instance. Will start it now.");

            try {
                platformAdapter.get().startPostgresInstance(adapterInstanceInfo.getAdapterInstanceId());
                adapterInstanceInfo = orchestratorUtils.waitUntilPostgresInstanceHealthy(adapterInstanceInfo.getAdapterInstanceId());
                if (adapterInstanceInfo == null) {
                    throw new NoPrimaryException("Postgres primary was started but failed to become healthy! Con not start orchestration!");
                }
                log.info("Successfully started non-active Postgres primary!");
            } catch (NoPrimaryException e) {
                throw e;
            } catch (Exception e) {
                throw new NoPrimaryException("Failed to start non-active Postgres primary! Con not start orchestration!", e);
            }
        }

        PostgresCombinedInstanceInfo primaryInstanceInfo = PostgresCombinedInstanceInfo
                .builder()
                .adapter(adapterInstanceInfo)
                .persisted(primaryPersistedInstanceInfo)
                .build();

        log.info("Primary is up and running!");

        return primaryInstanceInfo;
    }
}
