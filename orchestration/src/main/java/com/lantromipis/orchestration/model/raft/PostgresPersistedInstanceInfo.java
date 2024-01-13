package com.lantromipis.orchestration.model.raft;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Model describing persisted info about concrete Postgres node.
 * This info must be persisted, so data won't be lost between PgFacade restarts.
 * Data is shared among PgFacade cluster.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostgresPersistedInstanceInfo {
    /**
     * Unique Postgres node instanceId used among PgFacade. This instance is used by PostgresOrchestrator.
     */
    private UUID instanceId;
    /**
     * Adapter-specific identifier used only by OrchestrationAdapter.
     */
    private String adapterIdentifier;
    /**
     * Server name which is used for settings like cluster_name, synchronous_standby_names, etc.
     */
    private String serverName;
    /**
     * Replication slot which is used in case this instance is standby.
     */
    private String replicationSlotName;
    /**
     * Indicates if node is primary.
     */
    private boolean primary;
}
