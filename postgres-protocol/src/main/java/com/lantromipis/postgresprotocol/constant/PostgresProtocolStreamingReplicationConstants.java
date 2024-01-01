package com.lantromipis.postgresprotocol.constant;

public class PostgresProtocolStreamingReplicationConstants {

    public static final byte X_LOG_DATA_MESSAGE_START_CHAR = 'w';

    public static final byte PRIMARY_KEEPALIVE_MESSAGE_START_CHAR = 'k';
    public static final byte STANDBY_STATUS_UPDATE_MESSAGE_START_CHAR = 'r';

    public static final int X_LOG_DATA_MESSAGE_PREAMBLE_LENGTH = 24;
    public static final int PRIMARY_KEEPALIVE_CONTENT_LENGTH = 17;
    public static final int STANDBY_STATUS_UPDATE_MESSAGE_LENGTH = 34;

    public static final String IDENTIFY_SYSTEM_TIMELINE_COLUMN_NAME = "timeline";
    public static final String IDENTIFY_SYSTEM_X_LOG_POS_COLUMN_NAME = "xlogpos";

    public static final String TIMELINE_HISTORY_FILENAME_COLUMN_NAME = "filename";
    public static final String TIMELINE_HISTORY_CONTENT_COLUMN_NAME = "content";

    public static final String READ_REPLICATION_SLOT_SLOT_TYPE_COLUMN_NAME = "slot_type";
    public static final String READ_REPLICATION_SLOT_RESTART_LSN_COLUMN_NAME = "restart_lsn";
    public static final String READ_REPLICATION_SLOT_RESTART_TLI_COLUMN_NAME = "restart_tli";

    public static final String CREATE_REPLICATION_SLOT_SLOT_NAME_COLUMN_NAME = "slot_name";
    public static final String CREATE_REPLICATION_SLOT_CONSISTENT_POINT_COLUMN_NAME = "consistent_point";
    public static final String CREATE_REPLICATION_SLOT_SNAPSHOT_NAME_COLUMN_NAME = "snapshot_name";
    public static final String CREATE_REPLICATION_SLOT_OUTPUT_PLUGIN_COLUMN_NAME = "output_plugin";

    public static final String IDENTIFY_SYSTEM_QUERY = "IDENTIFY_SYSTEM";
    public static final String TIMELINE_HISTORY_QUERY = "TIMELINE_HISTORY";
    public static final String READ_REPLICATION_SLOT_QUERY = "READ_REPLICATION_SLOT";
    public static final String CREATE_REPLICATION_SLOT_QUERY = "CREATE_REPLICATION_SLOT";

    public static final String CREATE_REPLICATION_SLOT_QUERY_OPTION_PHYSICAL = "PHYSICAL";
    public static final String CREATE_REPLICATION_SLOT_QUERY_OPTION_RESERVE_WAL_ABOVE_PG_15 = "(RESERVE_WAL)";
    public static final String CREATE_REPLICATION_SLOT_QUERY_OPTION_RESERVE_WAL_BELOW_PG_15 = "RESERVE_WAL";


    private PostgresProtocolStreamingReplicationConstants() {
    }
}
