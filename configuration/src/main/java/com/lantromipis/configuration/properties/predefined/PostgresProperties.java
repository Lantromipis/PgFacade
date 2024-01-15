package com.lantromipis.configuration.properties.predefined;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "pg-facade.postgres")
public interface PostgresProperties {

    UserProperties users();

    interface UserProperties {
        UserCredentialsProperties pgFacade();

        UserCredentialsProperties replication();

        interface UserCredentialsProperties {
            String username();

            String password();

            String database();
        }
    }
}
