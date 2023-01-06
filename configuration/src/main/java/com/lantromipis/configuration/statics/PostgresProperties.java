package com.lantromipis.configuration.statics;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "pg-facade.postgres")
public interface PostgresProperties {
    String pgFacadeUser();

    String pgFacadePassword();

    String pgFacadeDatabase();

    interface HealthCheckProperties {
        int interval();
    }
}
