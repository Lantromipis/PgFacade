package com.lantromipis.configuration.exception;

public class ConfigurationInitializationException extends RuntimeException {
    public ConfigurationInitializationException() {
    }

    public ConfigurationInitializationException(String message) {
        super(message);
    }

    public ConfigurationInitializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfigurationInitializationException(Throwable cause) {
        super(cause);
    }

    public ConfigurationInitializationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
