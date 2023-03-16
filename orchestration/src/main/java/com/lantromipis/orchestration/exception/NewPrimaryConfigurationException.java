package com.lantromipis.orchestration.exception;

public class NewPrimaryConfigurationException extends RuntimeException {
    public NewPrimaryConfigurationException() {
    }

    public NewPrimaryConfigurationException(String message) {
        super(message);
    }

    public NewPrimaryConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public NewPrimaryConfigurationException(Throwable cause) {
        super(cause);
    }

    public NewPrimaryConfigurationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
