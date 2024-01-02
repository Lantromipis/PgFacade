package com.lantromipis.orchestration.constant;

import java.util.regex.Pattern;

public class CommandsConstants {

    public static final String PG_CTL_RECOVERY_TIMEOUT = "86400";

    public static final String PG_RECEIVE_WAL_COMMAND = "pg_receivewal";
    public static final String PG_RECEIVE_WAL_COMMAND_TARGET_DIR_KEY = "-D";
    public static final String PG_RECEIVE_WAL_COMMAND_HOST_KEY = "-h";
    public static final String PG_RECEIVE_WAL_COMMAND_PORT_KEY = "-p";
    public static final String PG_RECEIVE_WAL_COMMAND_USER_KEY = "-U";
    public static final String PG_RECEIVE_WAL_COMMAND_PASSWORD_KEY = "-w";
    public static final String PG_RECEIVE_WAL_COMMAND_NO_LOOP_KEY = "-n";

    public static final String PG_BASE_BACKUP_COMMAND = "pg_basebackup";
    public static final String PG_BASE_BACKUP_COMMAND_TARGET_DIR_KEY = "-D";
    public static final String PG_BASE_BACKUP_COMMAND_HOST_KEY = "-h";
    public static final String PG_BASE_BACKUP_COMMAND_PORT_KEY = "-p";
    public static final String PG_BASE_BACKUP_COMMAND_USERNAME_KEY = "-U";
    public static final String PG_BASE_BACKUP_COMMAND_PASSWORD_KEY = "-w";
    public static final String PG_BASE_BACKUP_BACKUP_LABEL_FILE_NAME = "backup_label";
    public static final Pattern PG_BASE_BACKUP_BACKUP_LABEL_WAL_FILE_NAME_PATTERN = Pattern.compile(".*file ([^)]*).*");

    private CommandsConstants() {
    }
}
