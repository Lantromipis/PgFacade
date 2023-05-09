package com.lantromipis.helper;

import com.lantromipis.model.ConfigurationInfo;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
@ApplicationScoped
public class PostgresConfigurationHelper {

    private static final String PG_FACADE_POSTGRESQL_CONF_FILE_NAME = "postgresql.pgfacade.conf";

    public void configure(String address, int port, String superDatabase, String superUsername, String superPassword, ConfigurationInfo configurationInfo) throws SQLException {
        Connection superuserConnectionInSuperDB = createConnection(
                address,
                port,
                superDatabase,
                superUsername,
                superPassword
        );

        // create PgFacade user
        superuserConnectionInSuperDB.createStatement().executeUpdate("CREATE USER \"" + configurationInfo.getPgFacadeUsername() + "\" WITH ENCRYPTED PASSWORD '" + configurationInfo.getPgFacadePassword() + "'");

        if (configurationInfo.isCreateReplicationUser()) {
            // create replication user
            superuserConnectionInSuperDB.createStatement().executeUpdate("CREATE ROLE \"" + configurationInfo.getReplicationUsername() + "\" WITH REPLICATION LOGIN ENCRYPTED PASSWORD '" + configurationInfo.getReplicationPassword() + "'");
        }

        // grant some predefined roles to PgFacade user
        grantRoleToUser(superuserConnectionInSuperDB, "pg_read_all_settings", configurationInfo.getPgFacadeUsername());

        // personal database for PgFacade user
        createDatabase(superuserConnectionInSuperDB, configurationInfo.getPgFacadeDatabase(), configurationInfo.getPgFacadeUsername());

        // create custom PgFacade configuration file
        String pgFacadeCustomConfigFile = getPgFacadePostgresqlConfFilePath(superuserConnectionInSuperDB);
        superuserConnectionInSuperDB.createStatement().executeUpdate("CREATE TEMP TABLE pg_facade_temp (lines text)");
        superuserConnectionInSuperDB.createStatement().executeUpdate("COPY pg_facade_temp TO '" + pgFacadeCustomConfigFile + "'");

        // include custom PgFacade configuration file to postgresql.conf
        String postgresqlConfLocation = getOriginalPostgresqlConfFilePath(superuserConnectionInSuperDB);
        String endLineOfPostgreslConf = "include_if_exists = ''" + pgFacadeCustomConfigFile + "''";
        String cmd = "echo \"" + endLineOfPostgreslConf + "\" >> " + postgresqlConfLocation;
        log.info("Cmd {}", cmd);
        superuserConnectionInSuperDB.createStatement().executeUpdate("COPY pg_facade_temp (lines) FROM PROGRAM '" + cmd + "'");

        // reload conf
        superuserConnectionInSuperDB.createStatement().execute("SELECT pg_reload_conf()");

        // now superuser connection in its database is not needed.
        superuserConnectionInSuperDB.close();

        // changing database for superuser because we need to grant execute on some functions in PgFacade user database.
        // Otherwise, PgFacade user won't be able to call such functions from its database.
        Connection superuserConnectionInPgFacadeDB = createConnection(
                address,
                port,
                configurationInfo.getPgFacadeDatabase(),
                superUsername,
                superPassword
        );

        // grant execute on some functions.
        grantExecuteOnFunction(superuserConnectionInPgFacadeDB, "pg_promote", configurationInfo.getPgFacadeUsername());
        grantExecuteOnFunction(superuserConnectionInPgFacadeDB, "pg_show_all_file_settings()", configurationInfo.getPgFacadeUsername());
        grantExecuteOnFunction(superuserConnectionInPgFacadeDB, "pg_reload_conf()", configurationInfo.getPgFacadeUsername());

        // grant select on pg_file_settings to PgFacade, so it will be possible to validate settings
        superuserConnectionInPgFacadeDB.createStatement().executeUpdate("GRANT SELECT ON pg_file_settings TO \"" + configurationInfo.getPgFacadeUsername() + "\"");

        // PgFacade user needs access to pg_authid, so PgFacade will be able to check credentials automatically
        superuserConnectionInPgFacadeDB.createStatement().executeUpdate("GRANT SELECT ON TABLE pg_authid TO \"" + configurationInfo.getPgFacadeUsername() + "\"");

        // now superuser connection in PgFacade database is not needed.
        superuserConnectionInPgFacadeDB.close();
    }

    private Connection createConnection(String address, int port, String database, String username, String password) throws SQLException {
        String jdbcUrl = "jdbc:postgresql://"
                + address
                + ":"
                + port
                + "/"
                + database;

        return DriverManager.getConnection(
                jdbcUrl,
                username,
                password
        );
    }

    private void createDatabase(Connection connection, String databaseName, String ownerUsername) throws SQLException {
        connection.createStatement().executeUpdate("CREATE DATABASE \"" + databaseName + "\" WITH OWNER \"" + ownerUsername + "\"");
    }

    private void grantRoleToUser(Connection connection, String role, String username) throws SQLException {
        connection.createStatement().executeUpdate("GRANT " + role + " TO \"" + username + "\"");
    }

    private void grantExecuteOnFunction(Connection connection, String function, String username) throws SQLException {
        connection.createStatement().executeUpdate("GRANT EXECUTE ON FUNCTION " + function + " TO \"" + username + "\"");
    }

    private String getPgFacadePostgresqlConfFilePath(Connection connection) throws SQLException {
        ResultSet resultSet = connection.createStatement().executeQuery("SELECT setting FROM pg_settings WHERE name = 'data_directory'");
        resultSet.next();

        return resultSet.getString(1) + "/" + PG_FACADE_POSTGRESQL_CONF_FILE_NAME;
    }

    private String getOriginalPostgresqlConfFilePath(Connection connection) throws SQLException {
        ResultSet resultSet = connection.createStatement().executeQuery("SELECT setting FROM pg_settings WHERE name = 'config_file'");
        resultSet.next();

        return resultSet.getString(1);
    }
}
