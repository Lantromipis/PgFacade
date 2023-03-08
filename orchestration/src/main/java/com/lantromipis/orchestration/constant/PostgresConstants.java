package com.lantromipis.orchestration.constant;

import lombok.*;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class PostgresConstants {

    public static final String PG_HBA_CONF_START_LINE = "# TYPE  DATABASE        USER            ADDRESS                 METHOD";
    public static final String PG_FACADE_POSTGRESQL_CONF_START_LINE = "# do not modify this file! It is managed by PgFacade!";

    public static final String PG_HBA_CONF_REPLICATION_DB = "replication";

    public static final String PG_HBA_CONF_ALL = "all";

    public static final String PG_DATA_ENV_VAR = "PGDATA";

    //settings
    public static final String PRIMARY_CONN_INFO_SETTING_NAME = "primary_conninfo";
    public static final String HBA_FILE_SETTING_NAME = "hba_file";
    public static final String DATA_DIRECTORY_SETTING_NAME = "data_directory";
    public static final String CONFIG_FILE_SETTING_NAME = "config_file";
    public static final String MAX_CONNECTIONS_SETTING_NAME = "max_connections";
    public static final String WAL_KEEP_SIZE_SETTING_NAME = "wal_keep_size";
    public static final String WAL_KEEP_SEGMENTS_SETTING_NAME = "wal_keep_segments";

    public static final List<String> FORBIDDEN_TO_CHANGE_SETTINGS_NAMES = List.of(
            PRIMARY_CONN_INFO_SETTING_NAME,
            "wal_level",
            "hot_standby",
            "archive_command",
            "archive_mode",
            "archive_cleanup_command",
            "archive_library"
    );
    public static final Set<String> UNMODIFIABLE_SETTINGS_CONTEXT_NAMES = Set.of("internal");
    public static final Set<String> RESTART_REQUIRED_SETTINGS_CONTEXT_NAMES = Set.of("postmaster");

    // Patterns and formats
    public static final String CONF_FILE_LINE_FORMAT = "%s = %s";
    public static final Pattern CONF_FILE_LINE_PATTERN = Pattern.compile("^([^ ]*) *= *(.*)$");
    public static final Pattern SHOW_SERVER_VERSION_PATTERN = Pattern.compile("([^ ]*).*");


    // https://www.postgresql.org/docs/current/auth-pg-hba-conf.html
    @RequiredArgsConstructor
    public enum PgHbaConfHost {
        HOST("host"),
        LOCAL("local"),
        HOST_SSL("hostssl"),
        HOST_NO_SSL("hostnossl"),
        HOST_GSS_ENC("hostgssenc"),
        HOST_NO_GSS_ENC("hostnogssenc");

        @Getter
        private final String value;
    }

    // https://www.postgresql.org/docs/current/auth-pg-hba-conf.html
    @RequiredArgsConstructor
    public enum PgHbaConfAuthMethod {
        TRUST("trust"),
        REJECT("reject"),
        SCRAM_SHA_256("scram-sha-256"),
        MD5("md5"),
        PASSWORD("password"),
        GSS("gss"),
        SSPI("sspi"),
        IDENT("ident"),
        PEER("peer"),
        LDAP("ldap"),
        RADIUS("radius"),
        CERT("cert"),
        PAM("pam"),
        VSD("bsd");

        @Getter
        private final String value;
    }

    private PostgresConstants() {
    }
}
