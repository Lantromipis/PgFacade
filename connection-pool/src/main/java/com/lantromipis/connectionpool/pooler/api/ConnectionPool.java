package com.lantromipis.connectionpool.pooler.api;

import com.lantromipis.connectionpool.model.PooledConnectionWrapper;
import com.lantromipis.connectionpool.model.StartupMessageInfo;
import com.lantromipis.connectionpool.model.auth.PoolAuthInfo;
import com.lantromipis.connectionpool.model.stats.ConnectionPoolStats;

import java.util.function.Consumer;

public interface ConnectionPool {
    void initialize();

    /**
     * Returns primary Postgres connection. Connection must be returned to pool when not needed!
     *
     * @param startupMessageInfo info about connection that was received from client in startup message
     * @param primary            If true, then connection to primary will be returned. If false, then connection to standby will be returned.
     * @param poolAuthInfo       info about auth in case new primary connection will be created
     * @param readyCallback      callback which will be called when method completed. May contain null if pool failed to provide connection.
     *                           Otherwise, callback will provide wrapper containing connection to primary or null if failed to acquire connection. Wrapper contains Netty channel and callback for returning connection.
     */
    void getPostgresConnection(StartupMessageInfo startupMessageInfo, boolean primary, PoolAuthInfo poolAuthInfo, Consumer<PooledConnectionWrapper> readyCallback);

    ConnectionPoolStats getStats();
}
