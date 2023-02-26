package com.lantromipis.configuration.producers;

import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@ApplicationScoped
public class RuntimePrimaryConnectionProducer {

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    PostgresProperties postgresProperties;

    public Connection createNewPgFacadeUserConnectionToCurrentPrimary() throws SQLException {
        String jdbcUrl = "jdbc:postgresql://"
                + clusterRuntimeProperties.getMasterHostAddress()
                + ":"
                + clusterRuntimeProperties.getMasterPort()
                + "/"
                + postgresProperties.users().pgFacade().database();

        return DriverManager.getConnection(
                jdbcUrl,
                postgresProperties.users().pgFacade().username(),
                postgresProperties.users().pgFacade().password()
        );
    }
}
