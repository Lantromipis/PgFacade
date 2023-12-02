package com.lantromipis.configuration.producers;

import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;

@ApplicationScoped
public class RuntimePostgresConnectionProducer {

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    PostgresProperties postgresProperties;

    public Connection createNewPgFacadeUserConnectionToCurrentPrimary() throws SQLException {
        RuntimePostgresInstanceInfo instanceInfo = clusterRuntimeProperties.getPrimaryInstanceInfo();
        if (instanceInfo == null) {
            return null;
        }

        return createNewPgFacadeUserConnectionToInstance(instanceInfo.getInstanceId());
    }

    public Connection createNewPgFacadeUserConnectionToInstance(UUID instanceId) throws SQLException {
        if (instanceId == null) {
            return null;
        }

        RuntimePostgresInstanceInfo runtimePostgresInstanceInfo = clusterRuntimeProperties.getAllPostgresInstancesInfos().get(instanceId);
        if (runtimePostgresInstanceInfo == null) {
            return null;
        }

        String jdbcUrl = "jdbc:postgresql://"
                + runtimePostgresInstanceInfo.getAddress()
                + ":"
                + runtimePostgresInstanceInfo.getPort()
                + "/"
                + postgresProperties.users().pgFacade().database();

        return DriverManager.getConnection(
                jdbcUrl,
                postgresProperties.users().pgFacade().username(),
                postgresProperties.users().pgFacade().password()
        );
    }
}
