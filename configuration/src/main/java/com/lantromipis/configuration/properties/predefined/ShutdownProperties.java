package com.lantromipis.configuration.properties.predefined;

import io.smallrye.config.ConfigMapping;

import java.time.Duration;
import java.util.Map;

@ConfigMapping(prefix = "pg-facade.shutdown")
public interface ShutdownProperties {
    boolean awaitClients();

    Duration waitForClientsDuration();
}
