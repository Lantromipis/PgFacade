package com.lantromipis.orchestration.util;

import com.lantromipis.configuration.properties.constant.PostgresConstants;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.orchestration.constant.CommandsConstants;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
public class PostgresUtils {

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    PostgresProperties postgresProperties;

    public String getShellPgCtlToStopPostgres(String pgDataDirPath) {
        return "pg_ctl stop -D " + pgDataDirPath + " ; rm -f " + pgDataDirPath + "/recovery.signal";
    }

    public String getShellPgCtlToStartRecovery(String pgDataDirPath, String dirWithWalFilesPath) {
        return "PGDATA=" + pgDataDirPath + " ; "
                + "pg_ctl start -s"
                + " -D " + pgDataDirPath
                + " -o \"-c restore_command='cp " + dirWithWalFilesPath + "/%f %p'\""
                + " -t " + CommandsConstants.PG_CTL_RECOVERY_TIMEOUT
                // sleeping to give headroom for database after startup
                + " 1> /dev/null ; sleep 10";
    }

    public String getShellCommandToPrepareForRecovery(String pgDataDirPath) {
        return "rm -rf " + pgDataDirPath + "/pg_wal/* ; "
                + "touch " + pgDataDirPath + "/recovery.signal ; "
                + "chown -R postgres:postgres " + pgDataDirPath + " ; "
                + "chmod 700 " + pgDataDirPath;
    }

    public Map<String, String> getDefaultSettings(int version) {
        Map<String, String> settings = new HashMap<>();

        addWalKepSetting(settings, version, postgresProperties.defaultSettings().maxWalKeepCount());

        return settings;
    }

    public void addWalKepSetting(Map<String, String> settings, int version, int walKeepCount) {
        if (version >= PostgresConstants.PG_VERSION_13_NUM) {
            settings.put(
                    PostgresConstants.WAL_KEEP_SIZE_SETTING_NAME,
                    walKeepCount * 16 + "MB"
            );
        } else {
            settings.put(
                    PostgresConstants.WAL_KEEP_SEGMENTS_SETTING_NAME,
                    String.valueOf(walKeepCount)
            );
        }
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
