package com.lantromipis.orchestration.constant;

public class CommandsConstants {

    public static final String POSTGRES_COMMAND = "postgres";
    public static final String POSTGRES_COMMAND_PARAMETER_KEY = "-c";

    public static final String PG_BASE_BACKUP_COMMAND = "pg_basebackup";
    public static final String PG_BASE_BACKUP_COMMAND_TARGET_DIR_KEY = "-D";
    public static final String PG_BASE_BACKUP_COMMAND_HOST_KEY = "-h";
    public static final String PG_BASE_BACKUP_COMMAND_PORT_KEY = "-p";
    public static final String PG_BASE_BACKUP_COMMAND_USERNAME_KEY = "-U";
    public static final String PG_BASE_BACKUP_COMMAND_PASSWORD_KEY = "-w";

    private CommandsConstants() {
    }
}
