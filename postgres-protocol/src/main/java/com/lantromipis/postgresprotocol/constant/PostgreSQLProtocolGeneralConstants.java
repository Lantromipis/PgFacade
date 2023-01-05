package com.lantromipis.postgresprotocol.constant;

public class PostgreSQLProtocolGeneralConstants {
    public static final byte DELIMITER_BYTE = 0;
    public static final String DELIMITER_BYTE_CHAR = "\0";
    public static final int INITIAL_ENCRYPTION_REQUEST_MESSAGE_FIRST_INT = 8;
    public static final int AUTH_OK = 0;
    public static final int SASL_AUTH_INT_MARKER = 10;
    public static final int SASL_AUTH_CHALLENGE_MARKER = 11;
    public static final int SASL_AUTH_COMPLETED_MARKER = 12;
    public static final String ENCRYPTION_NOT_SUPPORTED_RESPONSE_MESSAGE = "N";
    public static final String STARTUP_PARAMETER_USER = "user";
    public static final String STARTUP_PARAMETER_DATABASE = "database";
    public static final char CLIENT_PASSWORD_RESPONSE_START_CHAR = 'p';

    //message start chars
    public static final char AUTH_REQUEST_START_CHAR = 'R';
    public static final char ERROR_MESSAGE_START_CHAR = 'E';
    public static final char PARAMETER_STATUS_MESSAGE_START_CHAR = 'S';
    public static final char READY_FOR_QUERY_MESSAGE_START_CHAR = 'Z';
    public static final char TERMINATION_MESSAGE_START_CHAR = 'X';


    //ready for query
    public static final char READY_FOR_QUERY_TRANSACTION_IDLE = 'I';
}
