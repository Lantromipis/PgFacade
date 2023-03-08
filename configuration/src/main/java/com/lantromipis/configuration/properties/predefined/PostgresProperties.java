package com.lantromipis.configuration.properties.predefined;

import io.smallrye.config.ConfigMapping;

import java.util.Map;

@ConfigMapping(prefix = "pg-facade.postgres")
public interface PostgresProperties {

    ReplicationProperties replication();

    UserProperties users();

    interface ReplicationProperties {
        int maxWalKeepCount();
    }

    interface UserProperties {
        UserCredentialsProperties superuser();

        UserCredentialsProperties pgFacade();

        UserCredentialsProperties replication();

        interface UserCredentialsProperties {
            String username();

            String password();

            String database();
        }
    }
}
