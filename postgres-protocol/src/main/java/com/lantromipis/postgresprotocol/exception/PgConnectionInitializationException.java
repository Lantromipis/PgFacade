package com.lantromipis.postgresprotocol.exception;

public class PgConnectionInitializationException extends Exception {
    public PgConnectionInitializationException() {
    }

    public PgConnectionInitializationException(String message) {
        super(message);
    }

    public PgConnectionInitializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public PgConnectionInitializationException(Throwable cause) {
        super(cause);
    }

    public PgConnectionInitializationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
