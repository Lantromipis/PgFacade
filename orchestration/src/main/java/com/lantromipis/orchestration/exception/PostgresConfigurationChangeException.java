package com.lantromipis.orchestration.exception;

public class PostgresConfigurationChangeException extends RuntimeException {
    public PostgresConfigurationChangeException() {
    }

    public PostgresConfigurationChangeException(String message) {
        super(message);
    }

    public PostgresConfigurationChangeException(String message, Throwable cause) {
        super(message, cause);
    }

    public PostgresConfigurationChangeException(Throwable cause) {
        super(cause);
    }

    public PostgresConfigurationChangeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
