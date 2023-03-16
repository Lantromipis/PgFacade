package com.lantromipis.connectionpool.pooler.api;

import com.lantromipis.connectionpool.model.PooledConnectionWrapper;
import com.lantromipis.connectionpool.model.StartupMessageInfo;
import com.lantromipis.connectionpool.model.auth.AuthAdditionalInfo;

public interface ConnectionPool {
    void initialize();

    /**
     * Returns primary Postgres connection. Must be returned to pool when not needed!
     *
     * @param startupMessageInfo info about connection that was received from client in startup message
     * @param authAdditionalInfo info about auth in case new primary connection will be created
     * @return wrapper containing connection to primary or null if failed to acquire connection. Wrapper contains Netty channel and callback for returning connection.
     */
    PooledConnectionWrapper getPrimaryConnection(StartupMessageInfo startupMessageInfo, AuthAdditionalInfo authAdditionalInfo);
}
