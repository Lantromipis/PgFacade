package com.lantromipis.proxy.service.api;

import com.lantromipis.proxy.model.MessageLoadBalancingCheckResult;

public interface PgFrontendMessagesService {

    /**
     * Check if SQL statement can be load-balanced. Does not take into account if transaction is already in progress
     *
     * @return check result indicating if SQL statement can or cannot be load-balanced
     */
    MessageLoadBalancingCheckResult checkSQLStatement(String sqlStatement);
}
