package com.lantromipis.proxy.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageLoadBalancingCheckResult {
    private boolean canLoadBalance;

    private boolean isTransactionStart;
    private boolean isTransactionCommit;
    private boolean isTransactionRollback;

    // https://www.postgresql.org/docs/current/sql-set-transaction.html
    private boolean isSetTransactionCharacteristicsStatement;

    /**
     * Present if statement is BEGIN or SET TRANSACTION (isTransactionStart == true || isSetTransactionCharacteristicsStatement == true)
     */
    private boolean isSerializableIsolationLevelSet;
}
