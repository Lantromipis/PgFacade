package com.lantromipis.orchestration.exception;

public class NewMasterConfigurationException extends RuntimeException {
    public NewMasterConfigurationException() {
    }

    public NewMasterConfigurationException(String message) {
        super(message);
    }

    public NewMasterConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public NewMasterConfigurationException(Throwable cause) {
        super(cause);
    }

    public NewMasterConfigurationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
