package com.lantromipis.postgresprotocol.constant;

public class PostgresProtocolGeneralConstants {
    public static final byte DELIMITER_BYTE = 0;
    public static final String DELIMITER_BYTE_CHAR = "\0";
    public static final int MESSAGE_LENGTH_BYTES_COUNT = 4;
    public static final int MESSAGE_MARKER_AND_LENGTH_BYTES_COUNT = MESSAGE_LENGTH_BYTES_COUNT + 1;
    public static final int INITIAL_ENCRYPTION_REQUEST_MESSAGE_FIRST_INT = 8;
    public static final int AUTH_OK_MESSAGE_DATA = 0;
    public static final int AUTH_OK_MESSAGE_LENGTH = 8;
    public static final int READY_FOR_QUERY_MESSAGE_LENGTH = 6;
    public static final int SASL_AUTH_INT_MARKER = 10;
    public static final int SASL_AUTH_CHALLENGE_MARKER = 11;
    public static final int SASL_AUTH_COMPLETED_MARKER = 12;
    public static final byte[] ENCRYPTION_NOT_SUPPORTED_RESPONSE_MESSAGE = "N".getBytes();
    public static final String STARTUP_PARAMETER_USER = "user";
    public static final String STARTUP_PARAMETER_DATABASE = "database";
    public static final String STARTUP_PARAMETER_REPLICATION = "replication";

    //message start chars
    public static final byte AUTH_REQUEST_START_CHAR = 'R';
    public static final byte ERROR_MESSAGE_START_CHAR = 'E';
    public static final byte PARAMETER_STATUS_MESSAGE_START_CHAR = 'S';
    public static final byte READY_FOR_QUERY_MESSAGE_START_CHAR = 'Z';
    public static final byte CLIENT_TERMINATION_MESSAGE_START_CHAR = 'X';
    public static final byte CLIENT_PASSWORD_RESPONSE_START_CHAR = 'p';
    public static final byte ROW_DESCRIPTION_START_CHAR = 'T';
    public static final byte DATA_ROW_START_CHAR = 'D';
    public static final byte COMMAND_COMPLETE_START_CHAR = 'C';
    public static final byte COPY_BOTH_RESPONSE_START_CHAR = 'W';

    public static final byte COPY_DATA_START_CHAR = 'd';
    public static final byte COPY_DONE_START_CHAR = 'c';

    public static final byte QUERY_MESSAGE_START_BYTE = 'Q';
    public static final byte PARSE_MESSAGE_START_BYTE = 'P';
    public static final byte DESCRIBE_MESSAGE_START_BYTE = 'D';
    public static final byte BIND_MESSAGE_START_BYTE = 'B';
    public static final byte EXECUTE_MESSAGE_START_BYTE = 'E';
    public static final byte SYNC_MESSAGE_START_BYTE = 'S';


    //ready for query
    public static final byte READY_FOR_QUERY_TRANSACTION_IDLE = 'I';
}
