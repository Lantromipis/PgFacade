package com.lantromipis.orchestration.util;

import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.orchestration.constant.CommandsConstants;
import com.lantromipis.orchestration.constant.PostgresConstants;
import com.lantromipis.orchestration.model.PgSetting;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.math.BigDecimal;
import java.math.BigInteger;
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

    public PgSetting getWalKepSizeOrSegmentsSettings(double version, int walKeepCount) {
        if (version >= 13) {
            return PgSetting
                    .builder()
                    .name(PostgresConstants.WAL_KEEP_SIZE_SETTING_NAME)
                    .value(walKeepCount * 16 + "MB")
                    .build();
        } else {
            return PgSetting
                    .builder()
                    .name(PostgresConstants.WAL_KEEP_SEGMENTS_SETTING_NAME)
                    .value(String.valueOf(walKeepCount))
                    .build();
        }
    }

    public BigInteger calculateDifferenceBetweenWalFiles(String firstWal, String secondWal) {
        BigInteger firstWithoutTimeline = new BigInteger(firstWal.substring(8), 16);
        BigInteger secondWithoutTimeline = new BigInteger(secondWal.substring(8), 16);

        return firstWithoutTimeline.subtract(secondWithoutTimeline);
    }

    public Connection getConnectionToCurrentPrimary(String database, String username, String password) throws SQLException {
        return getConnectionToDatabase(
                clusterRuntimeProperties.getPrimaryInstanceInfo().getAddress(),
                clusterRuntimeProperties.getPrimaryInstanceInfo().getPort(),
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

    //TODO move to configuration/producers
    public String getPgPassFileContentForPrimary(PostgresProperties.UserProperties.UserCredentialsProperties userCredentialsProperties) {

        String database;
        if (userCredentialsProperties != postgresProperties.users().replication()) {
            database = userCredentialsProperties.database();
        } else {
            database = "*";
        }

        //TODO use subnet!!!
        return clusterRuntimeProperties.getPrimaryInstanceInfo().getAddress() + ":" +
                clusterRuntimeProperties.getPrimaryInstanceInfo().getPort() + ":" +
                database + ":" +
                userCredentialsProperties.username() + ":" +
                userCredentialsProperties.password();
    }

    public String getCommandToCreatePgPassFileForPrimary(PostgresProperties.UserProperties.UserCredentialsProperties userCredentialsProperties) {
        return "echo \"" + getPgPassFileContentForPrimary(userCredentialsProperties) + "\" > " + "$HOME/.pgpass ;" + " chmod 600 $HOME/.pgpass";
    }

    public String createPgBaseBackupCommand(String backupPath) {
        return CommandsConstants.PG_BASE_BACKUP_COMMAND
                + " "
                + CommandsConstants.PG_BASE_BACKUP_COMMAND_HOST_KEY + " " + clusterRuntimeProperties.getPrimaryInstanceInfo().getAddress()
                + " "
                + CommandsConstants.PG_BASE_BACKUP_COMMAND_PORT_KEY + " " + clusterRuntimeProperties.getPrimaryInstanceInfo().getPort()
                + " "
                + CommandsConstants.PG_BASE_BACKUP_COMMAND_USERNAME_KEY + " " + postgresProperties.users().replication().username()
                + " "
                + CommandsConstants.PG_BASE_BACKUP_COMMAND_TARGET_DIR_KEY + " " + backupPath
                + " "
                + CommandsConstants.PG_BASE_BACKUP_COMMAND_PASSWORD_KEY;
    }

    public String createPgReceiveWalCommand(UUID instanceId, String targetDir) {
        RuntimePostgresInstanceInfo runtimePostgresInstanceInfo = clusterRuntimeProperties.getAllPostgresInstancesInfos().get(instanceId);

        return CommandsConstants.PG_RECEIVE_WAL_COMMAND + " "
                + CommandsConstants.PG_RECEIVE_WAL_COMMAND_HOST_KEY + " " + runtimePostgresInstanceInfo.getAddress()
                + " "
                + CommandsConstants.PG_RECEIVE_WAL_COMMAND_PORT_KEY + " " + runtimePostgresInstanceInfo.getPort()
                + " "
                + CommandsConstants.PG_RECEIVE_WAL_COMMAND_USER_KEY + " " + postgresProperties.users().replication().username()
                + " "
                + CommandsConstants.PG_RECEIVE_WAL_COMMAND_TARGET_DIR_KEY + " " + targetDir
                + " "
                + CommandsConstants.PG_RECEIVE_WAL_COMMAND_PASSWORD_KEY
                + " "
                + CommandsConstants.PG_RECEIVE_WAL_COMMAND_NO_LOOP_KEY;
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
                "'host=%s port=%d user=%s password=%s'",
                clusterRuntimeProperties.getPrimaryInstanceInfo().getAddress(),
                clusterRuntimeProperties.getPrimaryInstanceInfo().getPort(),
                postgresProperties.users().replication().username(),
                postgresProperties.users().replication().password()
        );
    }
}
