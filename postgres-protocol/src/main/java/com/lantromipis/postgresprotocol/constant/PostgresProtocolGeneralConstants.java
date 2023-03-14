package com.lantromipis.postgresprotocol.constant;

public class PostgresProtocolGeneralConstants {
    public static final byte DELIMITER_BYTE = 0;
    public static final String DELIMITER_BYTE_CHAR = "\0";
    public static final int INITIAL_ENCRYPTION_REQUEST_MESSAGE_FIRST_INT = 8;
    public static final int AUTH_OK_MESSAGE_DATA = 0;
    public static final int AUTH_OK_MESSAGE_LENGTH = 8;
    public static final int READY_FOR_QUERY_MESSAGE_LENGTH = 5;
    public static final int SASL_AUTH_INT_MARKER = 10;
    public static final int SASL_AUTH_CHALLENGE_MARKER = 11;
    public static final int SASL_AUTH_COMPLETED_MARKER = 12;
    public static final String ENCRYPTION_NOT_SUPPORTED_RESPONSE_MESSAGE = "N";
    public static final String STARTUP_PARAMETER_USER = "user";
    public static final String STARTUP_PARAMETER_DATABASE = "database";

    //message start chars
    public static final byte AUTH_REQUEST_START_CHAR = 'R';
    public static final byte ERROR_MESSAGE_START_CHAR = 'E';
    public static final byte PARAMETER_STATUS_MESSAGE_START_CHAR = 'S';
    public static final byte READY_FOR_QUERY_MESSAGE_START_CHAR = 'Z';
    public static final byte CLIENT_TERMINATION_MESSAGE_START_CHAR = 'X';
    public static final byte CLIENT_PASSWORD_RESPONSE_START_CHAR = 'p';


    //ready for query
    public static final byte READY_FOR_QUERY_TRANSACTION_IDLE = 'I';
}
