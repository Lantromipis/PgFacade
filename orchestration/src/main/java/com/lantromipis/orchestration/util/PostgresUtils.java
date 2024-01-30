package com.lantromipis.orchestration.util;

import com.lantromipis.configuration.producers.RuntimePostgresConnectionProducer;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.orchestration.constant.CommandsConstants;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

@Slf4j
@ApplicationScoped
public class PostgresUtils {

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    PostgresProperties postgresProperties;

    @Inject
    RuntimePostgresConnectionProducer runtimePostgresConnectionProducer;

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
                "host=%s port=%d user=%s password=%s",
                clusterRuntimeProperties.getPrimaryInstanceInfo().getAddress(),
                clusterRuntimeProperties.getPrimaryInstanceInfo().getPort(),
                postgresProperties.users().replication().username(),
                postgresProperties.users().replication().password()
        );
    }

    public void createPhysicalReplicationSlot(Connection connection, String slotName) throws SQLException {
        if (slotName == null) {
            return;
        }

        PreparedStatement statement = connection.prepareStatement("SELECT pg_create_physical_replication_slot(?, true, false)");
        statement.setString(1, slotName);
        statement.executeQuery();
        statement.close();
    }

    public void dropPhysicalReplicationSlot(Connection connection, String slotName) throws SQLException {
        if (slotName == null) {
            return;
        }

        PreparedStatement statement = connection.prepareStatement("SELECT pg_drop_replication_slot(?)");
        statement.setString(1, slotName);
        statement.executeQuery();
        statement.close();
    }

    public boolean dropPhysicalReplicationSlotOnPrimarySafely(String slotName) {
        if (slotName == null) {
            return true;
        }
        try (Connection primaryConnection = runtimePostgresConnectionProducer.createNewPgFacadeUserConnectionToCurrentPrimary()) {
            dropPhysicalReplicationSlot(primaryConnection, slotName);
            return true;
        } catch (Exception ex) {
            log.error("Failed to drop physical replication slot! Be sure to remove it manually!", ex);
            return false;
        }
    }

    public String createPostgresServerName(UUID standbyInstanceId) {
        return "pgfacade-managed-postgres-" + standbyInstanceId.toString();
    }

    public String createPostgresReplicationSlotName(UUID standbyInstanceId) {
        return "pgfacade_standby_slot_" + standbyInstanceId.toString().replaceAll("-", "_");
    }
}
