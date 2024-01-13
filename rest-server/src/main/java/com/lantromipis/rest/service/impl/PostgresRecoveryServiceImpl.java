package com.lantromipis.rest.service.impl;

import com.lantromipis.orchestration.model.raft.PostgresPersistedInstanceInfo;
import com.lantromipis.orchestration.service.api.PostgresRestorationService;
import com.lantromipis.orchestration.service.api.raft.RaftStorage;
import com.lantromipis.rest.model.internal.PostgresRestoreSettings;
import com.lantromipis.rest.service.api.PostgresRecoveryService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

@Slf4j
@ApplicationScoped
public class PostgresRecoveryServiceImpl implements PostgresRecoveryService {


    @Inject
    PostgresRestorationService postgresRestorationService;

    @Inject
    RaftStorage raftStorage;

    @Override
    public void startRecoveryFromBackup(PostgresRestoreSettings settings) {
        String adapterIdentifier = postgresRestorationService.stopArchiverAndRestorePostgresFromBackup();

        if (settings.isSaveAsNewPrimary()) {
            // saving directly to Raft storage because Raft can not be used in recovery mode

            raftStorage.clearPostgresNodesInfos();
            raftStorage.savePostgresNodeInfo(
                    PostgresPersistedInstanceInfo
                            .builder()
                            .instanceId(UUID.randomUUID())
                            .primary(true)
                            .adapterIdentifier(adapterIdentifier)
                            .build()
            );
            raftStorage.deleteArchiveInfo();

            log.info("New master was created from backup. Adapter identifier is {}", adapterIdentifier);
        }

    }
}
