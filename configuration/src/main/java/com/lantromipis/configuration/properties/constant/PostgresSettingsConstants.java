package com.lantromipis.configuration.properties.constant;

import java.util.Set;

public class PostgresSettingsConstants {

    // pg_settings_column_names
    public static final String PG_SETTING_COLUMN_SETTING_NAME = "name";
    public static final String PG_SETTING_COLUMN_SETTING_VALUE = "setting";
    public static final String PG_SETTING_COLUMN_SETTING_CONTEXT = "context";
    public static final String PG_SETTING_COLUMN_SETTING_UNIT = "unit";

    //settings
    public static final String PRIMARY_CONN_INFO_SETTING_NAME = "primary_conninfo";
    public static final String MAX_CONNECTIONS_SETTING_NAME = "max_connections";
    public static final String SUPERUSER_RESERVED_CONNECTIONS_SETTING_NAME = "superuser_reserved_connections";
    public static final String SERVER_VERSION_NUM_SETTING_NAME = "server_version_num";
    public static final String WAL_SEGMENT_SIZE_SETTING_NAME = "wal_segment_size";
    public static final String PRIMARY_SLOT_NAME_SETTING_NAME = "primary_slot_name";
    public static final String CLUSTER_NAME_SETTING_NAME = "cluster_name";

    public static final int PG_VERSION_13_NUM = 130000;
    public static final int PG_VERSION_15_NUM = 150000;

    public static final Set<String> FORBIDDEN_TO_CHANGE_SETTINGS_NAMES = Set.of(
            PRIMARY_CONN_INFO_SETTING_NAME,
            "wal_level",
            "hot_standby",
            "archive_command",
            "archive_mode",
            "archive_cleanup_command",
            "archive_library",
            PRIMARY_SLOT_NAME_SETTING_NAME,
            CLUSTER_NAME_SETTING_NAME
    );
    public static final Set<String> UNMODIFIABLE_SETTINGS_CONTEXT_NAMES = Set.of("internal");
    public static final Set<String> RESTART_REQUIRED_SETTINGS_CONTEXT_NAMES = Set.of("postmaster");


    private PostgresSettingsConstants() {
    }
}
