package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.properties.constant.PostgresqlConfConstants;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.OrchestrationAdapter;
import com.lantromipis.orchestration.exception.NewMasterConfigurationException;
import com.lantromipis.orchestration.service.api.PostgresConfigurator;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.sql.*;
import java.util.List;

@Slf4j
@ApplicationScoped
public class PostgresConfiguratorImpl implements PostgresConfigurator {

    @Inject
    PostgresProperties postgresProperties;

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    OrchestrationAdapter orchestrationAdapter;

    @Override
    public void configureNewlyCreatedMaster() {
        log.info("Executing required start-up SQL statements on new master.");
        try {
            Connection superuserConnectionInSuperDB = createJdbcConnection(
                    postgresProperties.users().superuser().username(),
                    postgresProperties.users().superuser().password(),
                    postgresProperties.users().superuser().database()
            );

            PostgresProperties.UserProperties.UserCredentialsProperties pgFacadeUserProperties = postgresProperties.users().pgFacade();

            //create PgFacade user
            superuserConnectionInSuperDB.createStatement()
                    .executeUpdate("CREATE USER " + pgFacadeUserProperties.username()
                            + " WITH ENCRYPTED PASSWORD '" + pgFacadeUserProperties.password() + "'");

            //create replication user
            superuserConnectionInSuperDB.createStatement()
                    .executeUpdate("CREATE ROLE " + postgresProperties.users().replication().username()
                            + " WITH REPLICATION LOGIN ENCRYPTED PASSWORD '" + postgresProperties.users().replication().password() + "'");

            //grant default roles to PgFacade user, so it will be possible to change pg_hba.conf
            grantRoleToUser(superuserConnectionInSuperDB, "pg_read_server_files", pgFacadeUserProperties.username());
            grantRoleToUser(superuserConnectionInSuperDB, "pg_write_server_files", pgFacadeUserProperties.username());

            //grant execute on pg_reload_conf() to be able to reload config
            grantExecuteOnFunction(superuserConnectionInSuperDB, "pg_reload_conf()", pgFacadeUserProperties.username());

            superuserConnectionInSuperDB.createStatement()
                    .executeUpdate("GRANT SELECT ON TABLE pg_shadow TO " + pgFacadeUserProperties.username());

            //personal database for PgFacade user
            createDatabase(superuserConnectionInSuperDB, pgFacadeUserProperties.database(), pgFacadeUserProperties.username());

            //now we don't need superuser in his database.
            superuserConnectionInSuperDB.close();

            //changing database for superuser because we need to execute "GRANT EXECUTE ON pg_promote ..." in PgFacade's user database.
            //Otherwise, PgFacade user won't be able to call pg_promote from its database.
            Connection superuserConnectionInPgFacadeDB = createJdbcConnection(
                    postgresProperties.users().superuser().username(),
                    postgresProperties.users().superuser().password(),
                    postgresProperties.users().pgFacade().database()
            );

            //grant execute on pg_promote() to be able to promote standby
            grantExecuteOnFunction(superuserConnectionInPgFacadeDB, "pg_promote", pgFacadeUserProperties.username());

            //now we don't need superuser in PgFacade database.
            superuserConnectionInPgFacadeDB.close();

            //connection to PgFacade user's database.
            Connection pgfacadeUserConnection = createJdbcConnection(
                    postgresProperties.users().superuser().username(),
                    postgresProperties.users().superuser().password(),
                    postgresProperties.users().pgFacade().database()
            );

            //we need access to pg_authid so PgFacade will be able to check credentials automatically
            pgfacadeUserConnection.createStatement()
                    .executeUpdate("CREATE VIEW " + PostgresqlConfConstants.PG_AUTHID_VIEW_NAME + " AS SELECT * FROM pg_authid WHERE rolcanlogin = true");

            pgfacadeUserConnection.createStatement()
                    .executeUpdate("GRANT SELECT ON TABLE " + PostgresqlConfConstants.PG_AUTHID_VIEW_NAME + " TO " + pgFacadeUserProperties.username());

            //update pg_hba.conf
            replacePgHbaConf(pgfacadeUserConnection, orchestrationAdapter.getRequiredHbaConfLines());

            //finished configuration
            pgfacadeUserConnection.close();

            log.info("Finished executing required start-up SQL statements on new master.");

        } catch (Exception e) {
            throw new NewMasterConfigurationException("Failed to execute required start-up SQL for newly created master.", e);
        }
    }

    public void replacePgHbaConf(Connection connection, List<String> newLines) throws SQLException {
        String pgHbaConfFilePath = getPgHbaConfFilePath(connection);

        connection.createStatement()
                .executeUpdate("CREATE TEMP TABLE hba_temp (line TEXT)");

        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO hba_temp VALUES (?)");

        for (String line : newLines) {
            preparedStatement.setString(1, line);
            preparedStatement.addBatch();
        }

        preparedStatement.executeBatch();

        connection.createStatement()
                .execute("COPY hba_temp TO '" + pgHbaConfFilePath + "'");

        connection.createStatement()
                .executeUpdate("DROP TABLE  hba_temp");

        //TODO throw exception is reload unsuccessful
        connection.createStatement()
                .execute("SELECT pg_reload_conf()");
    }

    private Connection createJdbcConnection(String username, String password, String database) throws SQLException {
        String jdbcUrl = "jdbc:postgresql://"
                + clusterRuntimeProperties.getMasterHostAddress()
                + ":"
                + clusterRuntimeProperties.getMasterPort()
                + "/"
                + database;

        return DriverManager.getConnection(
                jdbcUrl,
                username,
                password
        );
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
