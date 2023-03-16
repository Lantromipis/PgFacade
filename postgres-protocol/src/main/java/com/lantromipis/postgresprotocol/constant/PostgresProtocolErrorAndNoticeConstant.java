package com.lantromipis.postgresprotocol.constant;

public class PostgresProtocolErrorAndNoticeConstant {

    // Message fields https://www.postgresql.org/docs/current/protocol-error-fields.html

    public static final byte SEVERITY_LOCALIZED_MARKER = 'S';
    public static final byte SEVERITY_NOT_LOCALIZED_MARKER = 'V';
    public static final byte SQLSTATE_CODE_MARKER = 'C';
    public static final byte MESSAGE_MARKER = 'M';

    // Error Codes https://www.postgresql.org/docs/current/errcodes-appendix.html
    public static final String INVALID_PASSWORD_SQLSTATE_ERROR_CODE = "28P01";

    // Severity
    public static final String FATAL_SEVERITY = "FATAL";

    // Messages
    public static final String AUTH_FAILED_MESSAGE_FORMAT = "password authentication failed for user \"%s\"";

    private PostgresProtocolErrorAndNoticeConstant() {
    }
}
