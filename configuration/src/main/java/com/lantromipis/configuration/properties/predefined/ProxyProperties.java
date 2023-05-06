package com.lantromipis.configuration.properties.predefined;

import io.smallrye.config.ConfigMapping;

import java.time.Duration;

@ConfigMapping(prefix = "pg-facade.proxy")
public interface ProxyProperties {
    int primaryPort();

    int standbyPort();

    int maxConnections();

    ConnectionPoolProperties connectionPool();

    InactiveClientsProperties inactiveClients();

    interface InactiveClientsProperties {
        boolean disconnect();

        Duration inactiveConnectionTimeout();

        Duration checkInterval();
    }

    interface ConnectionPoolProperties {
        boolean enabled();

        boolean awaitConnectionWhenPoolEmpty();

        Duration awaitConnectionWhenPoolEmptyTimeout();

        Duration cleanRealUsedConnectionTimeout();

        Duration acquireRealConnectionTimeout();

        Duration realConnectionAuthTimeout();

        Duration poolCleanupInterval();

        Duration redundantConnectionsLifetime();

        Duration connectionMaxAge();
    }
}
