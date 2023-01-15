package com.lantromipis.orchestration.orchestrator.impl;

import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.internaldatabaseusage.provider.api.DynamicMasterConnectionProvider;
import com.lantromipis.orchestration.exception.NewMasterConfigurationException;
import com.lantromipis.orchestration.orchestrator.api.PostgresConfigurator;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.units.qual.C;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.sql.*;

@Slf4j
@ApplicationScoped
public class PostgresConfiguratorImpl implements PostgresConfigurator {

    @Inject
    DynamicMasterConnectionProvider dynamicMasterConnectionProvider;

    @Inject
    PostgresProperties postgresProperties;

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Override
    public void configureNewlyCreatedMaster() {
        log.info("Executing required start-up SQL statements on new master.");
        try {
            String jdbcUrl =
                    "jdbc:postgresql://"
                            + clusterRuntimeProperties.getMasterHostAddress()
                            + ":"
                            + clusterRuntimeProperties.getMasterPort()
                            + "/"
                            + postgresProperties.users().superuser().database();

            Connection connection = DriverManager.getConnection(
                    jdbcUrl,
                    postgresProperties.users().superuser().username(),
                    postgresProperties.users().superuser().password()
            );

            PostgresProperties.UserProperties.UserCredentialsProperties pgFacadeUserProperties = postgresProperties.users().pgFacade();

            //create PgFacade user
            connection.createStatement()
                    .executeUpdate("CREATE USER " + pgFacadeUserProperties.username()
                            + " WITH ENCRYPTED PASSWORD '" + pgFacadeUserProperties.password() + "'");

            //create replication user
            connection.createStatement()
                    .executeUpdate("CREATE ROLE " + postgresProperties.users().replication().username()
                            + " WITH REPLICATION LOGIN ENCRYPTED PASSWORD '" + postgresProperties.users().replication().password() + "'");

            //grant default roles to PgFacade user, so it will be possible to change pg_hba.conf
            grantRoleToUser(connection, "pg_read_server_files", pgFacadeUserProperties.username());
            grantRoleToUser(connection, "pg_write_server_files", pgFacadeUserProperties.username());

            //grant execute on pg_reload_conf() to be able to reload config
            grantExecuteOnFunction(connection, "pg_reload_conf()", pgFacadeUserProperties.username());

            connection.createStatement()
                    .executeUpdate("GRANT SELECT ON TABLE pg_shadow TO " + pgFacadeUserProperties.username());

            //personal database for PgFacade user
            createDatabase(connection, pgFacadeUserProperties.database(), pgFacadeUserProperties.username());

            String pgHbaConfFilePath = getPgHbaConfFilePath(connection);


            //now we don't need superuser.
            connection.close();

            log.info("Finished executing required start-up SQL statements on new master.");

        } catch (Exception e) {
            throw new NewMasterConfigurationException("Failed to execute required start-up SQL for newly created master.", e);
        }
    }

    private String getPgHbaConfFilePath(Connection connection) throws SQLException {
        ResultSet resultSet = connection.createStatement()
                .executeQuery("SELECT setting FROM pg_settings WHERE name LIKE '%hba%'");
        resultSet.next();

        return resultSet.getString(1);
    }

    private void createDatabase(Connection connection, String databaseName, String ownerUsername) throws SQLException {
        connection.createStatement()
                .executeUpdate("CREATE DATABASE " + databaseName + " WITH OWNER " + ownerUsername);
    }

    private void grantRoleToUser(Connection connection, String role, String username) throws SQLException {
        connection.createStatement()
                .executeUpdate("GRANT " + role + " TO " + username);
    }

    private void grantExecuteOnFunction(Connection connection, String function, String username) throws SQLException {
        connection.createStatement()
                .executeUpdate("GRANT EXECUTE ON FUNCTION " + function + " TO " + username);
    }
}
