package com.lantromipis.model.copy;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Copy of com.lantromipis.orchestration.model.raft.PostgresPersistedInstanceInfo
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostgresPersistedInstanceInfoCopy {
    private UUID instanceId;
    private String adapterIdentifier;
    private boolean primary;
}
