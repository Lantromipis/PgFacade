package com.lantromipis.pooler.api;

import com.lantromipis.model.ConnectionInfo;
import com.lantromipis.model.common.AuthAdditionalInfo;
import io.netty.channel.Channel;

public interface ConnectionPool {
    void initialize();

    Channel getMasterConnection(ConnectionInfo connectionInfo, AuthAdditionalInfo authAdditionalInfo);

    void returnConnectionToPool(ConnectionInfo connectionInfo, Channel connection);
}
