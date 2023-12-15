package com.lantromipis.connectionpool.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PooledConnectionReturnParameters {
    /**
     * Execute SQL statements to cleanup connection before returning to pool (rollback, deallocate all, etc.).
     */
    boolean cleanup;
    /**
     * Force close primary connection. Main purpose is to use for switchover.
     */
    boolean terminate;
}
