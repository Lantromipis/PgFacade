package com.lantromipis.exception;

public class PostgresInstanceCreationException extends RuntimeException {
    public PostgresInstanceCreationException() {
    }

    public PostgresInstanceCreationException(String message) {
        super(message);
    }

    public PostgresInstanceCreationException(String message, Throwable cause) {
        super(message, cause);
    }

    public PostgresInstanceCreationException(Throwable cause) {
        super(cause);
    }

    public PostgresInstanceCreationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
