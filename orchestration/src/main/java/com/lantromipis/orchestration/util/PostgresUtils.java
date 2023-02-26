package com.lantromipis.orchestration.util;

import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.orchestration.constant.CommandsConstants;
import com.lantromipis.orchestration.constant.PostgresConstants;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;

@ApplicationScoped
public class PostgresUtils {

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    PostgresProperties postgresProperties;

    public Connection getConnectionForPgFacadeUserToCurrentPrimary() throws SQLException {
        return getConnectionForPgFacadeUser(
                clusterRuntimeProperties.getMasterHostAddress(),
                clusterRuntimeProperties.getMasterPort()
        );
    }

    public Connection getConnectionToCurrentPrimary(String database, String username, String password) throws SQLException {
        return getConnectionToDatabase(
                clusterRuntimeProperties.getMasterHostAddress(),
                clusterRuntimeProperties.getMasterPort(),
                database,
                username,
                password
        );
    }

    public Connection getConnectionForPgFacadeUser(String address, int port) throws SQLException {
        return getConnectionToDatabase(
                address,
                port,
                postgresProperties.users().pgFacade().database(),
                postgresProperties.users().pgFacade().username(),
                postgresProperties.users().pgFacade().password()
        );
    }

    public Connection getConnectionToDatabase(String address, int port, String database, String username, String password) throws SQLException {
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

    public String getPgPassFileContent(PostgresProperties.UserProperties.UserCredentialsProperties userCredentialsProperties) {

        String database;
        if (userCredentialsProperties != postgresProperties.users().replication()) {
            database = userCredentialsProperties.database();
        } else {
            database = "*";
        }

        return clusterRuntimeProperties.getMasterHostAddress() + ":" +
                clusterRuntimeProperties.getMasterPort() + ":" +
                database + ":" +
                userCredentialsProperties.username() + ":" +
                userCredentialsProperties.password();
    }

    public String getCommandToCreatePgPassFile(PostgresProperties.UserProperties.UserCredentialsProperties userCredentialsProperties) {
        return "echo \"" + getPgPassFileContent(userCredentialsProperties) + "\" > " + "$HOME/.pgpass ;" + " chmod 600 $HOME/.pgpass";
    }

    public String createPgBaseBackupCommand(String backupPath) {
        return CommandsConstants.PG_BASE_BACKUP_COMMAND + " " +
                CommandsConstants.PG_BASE_BACKUP_COMMAND_HOST_KEY + " " + clusterRuntimeProperties.getMasterHostAddress() + " " +
                CommandsConstants.PG_BASE_BACKUP_COMMAND_PORT_KEY + " " + clusterRuntimeProperties.getMasterPort() + " " +
                CommandsConstants.PG_BASE_BACKUP_COMMAND_USERNAME_KEY + " " + postgresProperties.users().replication().username() + " " +
                CommandsConstants.PG_BASE_BACKUP_COMMAND_TARGET_DIR_KEY + " " + backupPath + " " +
                CommandsConstants.PG_BASE_BACKUP_COMMAND_PASSWORD_KEY;
    }

    public String createRandomTableName(String prefix) {
        return prefix + Math.abs(UUID.randomUUID().getMostSignificantBits());
    }

    public String generatePgHbaConfLine(PostgresConstants.PgHbaConfHost hostType, String database, String user, String address, PostgresConstants.PgHbaConfAuthMethod authMethod) {
        String realAddress = PostgresConstants.PgHbaConfHost.LOCAL.equals(hostType) ? "" : address + " ";

        return hostType.getValue() + " "
                + database + " "
                + user + " "
                + realAddress
                + authMethod.getValue();
    }

    public String getPrimaryConnInfoSetting() {
        return String.format(
                "host=%s port=%d user=%s password=%s",
                clusterRuntimeProperties.getMasterHostAddress(),
                clusterRuntimeProperties.getMasterPort(),
                postgresProperties.users().replication().username(),
                postgresProperties.users().replication().password()
        );
    }
}
