package com.lantromipis.connectionpool.pooler.api;

import com.lantromipis.connectionpool.model.ConnectionInfo;
import com.lantromipis.connectionpool.model.common.AuthAdditionalInfo;
import io.netty.channel.Channel;

public interface ConnectionPool {
    void initialize();

    Channel getMasterConnection(ConnectionInfo connectionInfo, AuthAdditionalInfo authAdditionalInfo);

    void returnConnectionToPool(ConnectionInfo connectionInfo, Channel connection);
}
