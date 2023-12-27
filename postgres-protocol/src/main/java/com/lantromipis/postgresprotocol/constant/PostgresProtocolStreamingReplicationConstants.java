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

    private PostgresProtocolStreamingReplicationConstants() {
    }
}
