package com.lantromipis.orchestration.exception;

public class PostgresRestoreException extends RuntimeException {
    public PostgresRestoreException() {
    }

    public PostgresRestoreException(String message) {
        super(message);
    }

    public PostgresRestoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public PostgresRestoreException(Throwable cause) {
        super(cause);
    }

    public PostgresRestoreException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
