package com.lantromipis.configuration.predefined;

import io.smallrye.config.ConfigMapping;

import java.util.Map;

@ConfigMapping(prefix = "pg-facade.postgres")
public interface PostgresProperties {
    String pgFacadeUser();

    String pgFacadePassword();

    String pgFacadeDatabase();

    Map<String, String> postgresqlConfOverride();
}
