package com.lantromipis.configuration.predefined;

import io.smallrye.config.ConfigMapping;

import java.util.Map;

@ConfigMapping(prefix = "pg-facade.postgres")
public interface PostgresProperties {

    Map<String, String> postgresqlConfOverride();

    UserProperties users();

    interface UserProperties {
        UserCredentialsProperties pgFacade();

        UserCredentialsProperties replication();

        interface UserCredentialsProperties {
            String username();

            String password();

            String database();

            String pgHbaConfLine();
        }
    }
}
