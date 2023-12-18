package com.lantromipis.postgresprotocol.constant;

public class PostgresProtocolStreamingReplicationConstants {

    public static final byte X_LOG_DATA_MESSAGE_START_CHAR = 'w';

    public static final byte PRIMARY_KEEPALIVE_MESSAGE_START_CHAR = 'k';
    public static final byte STANDBY_STATUS_UPDATE_MESSAGE_START_CHAR = 'r';

    public static final int X_LOG_DATA_MESSAGE_PREAMBLE_LENGTH = 24;
    public static final int PRIMARY_KEEPALIVE_CONTENT_LENGTH = 17;
    public static final int STANDBY_STATUS_UPDATE_MESSAGE_LENGTH = 34;

    private PostgresProtocolStreamingReplicationConstants() {
    }
}
