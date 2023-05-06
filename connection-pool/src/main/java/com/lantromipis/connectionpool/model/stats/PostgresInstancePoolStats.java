package com.lantromipis.connectionpool.model.stats;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostgresInstancePoolStats {
    private UUID instanceId;
    private boolean isPrimary;
    private int connections;
}
