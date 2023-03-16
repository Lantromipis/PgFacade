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
     * Execute ROLLBACK SQL statement to close active transaction before returning connection to pool.
     */
    boolean rollback;
    /**
     * Force close primary connection. Main purpose is to use for switchover.
     */
    boolean terminate;
}
