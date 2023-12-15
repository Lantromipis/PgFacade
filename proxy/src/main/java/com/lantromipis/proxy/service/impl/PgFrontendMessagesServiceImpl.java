package com.lantromipis.proxy.service.impl;

import com.lantromipis.proxy.model.MessageLoadBalancingCheckResult;
import com.lantromipis.proxy.service.api.PgFrontendMessagesService;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class PgFrontendMessagesServiceImpl implements PgFrontendMessagesService {

    public MessageLoadBalancingCheckResult checkSQLStatement(String sqlStatement) {

        return null;
    }
}
