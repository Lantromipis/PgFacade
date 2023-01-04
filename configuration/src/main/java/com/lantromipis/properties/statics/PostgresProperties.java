package com.lantromipis.properties.statics;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "pg-facade.postgres")
public interface PostgresProperties {
    String pgFacadeUser();

    String pgFacadePassword();

    String pgFacadeDatabase();

    HealthCheckProperties healthcheck();

    interface HealthCheckProperties {
        long interval();

        long timeout();

        int retries();

        long startPeriod();

        String command();
    }
}
