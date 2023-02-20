package com.lantromipis.configuration.properties.predefined;

import io.smallrye.config.ConfigMapping;

import java.time.Duration;
import java.util.Map;

@ConfigMapping(prefix = "pg-facade.proxy")
public interface ProxyStaticProperties {
    int port();

    int maxConnections();

    InactiveClientsProperties inactiveClients();

    Map<String, String> parameterStatusOverride();

    interface InactiveClientsProperties {
        boolean disconnect();

        Duration inactiveConnectionTimeout();

        Duration checkInterval();
    }
}
