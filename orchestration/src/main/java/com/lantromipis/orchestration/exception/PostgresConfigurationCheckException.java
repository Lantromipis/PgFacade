package com.lantromipis.orchestration.exception;

public class PostgresConfigurationCheckException extends RuntimeException {
    public PostgresConfigurationCheckException() {
    }

    public PostgresConfigurationCheckException(String message) {
        super(message);
    }

    public PostgresConfigurationCheckException(String message, Throwable cause) {
        super(message, cause);
    }

    public PostgresConfigurationCheckException(Throwable cause) {
        super(cause);
    }

    public PostgresConfigurationCheckException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
