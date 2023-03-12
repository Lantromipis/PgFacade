package com.lantromipis.configuration.properties.predefined;

import io.smallrye.config.ConfigMapping;

import java.time.Duration;
import java.util.Map;

@ConfigMapping(prefix = "pg-facade.proxy")
public interface ProxyProperties {
    int port();

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

        Duration acquireRealConnectionTimeout();

        Duration realConnectionAuthTimeout();
    }
}
