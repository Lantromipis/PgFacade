package com.lantromipis.orchestration.util;

import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.orchestration.constant.CommandsConstants;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class PostgresUtils {

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    PostgresProperties postgresProperties;

    public String getPgPassFileContent(PostgresProperties.UserProperties.UserCredentialsProperties userCredentialsProperties) {

        return clusterRuntimeProperties.getMasterHostAddress() + ":" +
                clusterRuntimeProperties.getMasterPort() + ":" +
                userCredentialsProperties.database() + ":" +
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
}