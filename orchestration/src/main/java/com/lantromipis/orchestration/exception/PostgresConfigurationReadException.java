package com.lantromipis.orchestration.exception;

public class PostgresConfigurationReadException extends RuntimeException {
    public PostgresConfigurationReadException() {
    }

    public PostgresConfigurationReadException(String message) {
        super(message);
    }

    public PostgresConfigurationReadException(String message, Throwable cause) {
        super(message, cause);
    }

    public PostgresConfigurationReadException(Throwable cause) {
        super(cause);
    }

    public PostgresConfigurationReadException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
