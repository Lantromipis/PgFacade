package com.lantromipis.orchestration.exception;

public class PostgresInstanceStartException extends RuntimeException {
    public PostgresInstanceStartException() {
    }

    public PostgresInstanceStartException(String message) {
        super(message);
    }

    public PostgresInstanceStartException(String message, Throwable cause) {
        super(message, cause);
    }

    public PostgresInstanceStartException(Throwable cause) {
        super(cause);
    }

    public PostgresInstanceStartException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
