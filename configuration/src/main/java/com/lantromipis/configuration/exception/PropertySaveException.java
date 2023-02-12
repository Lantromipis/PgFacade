package com.lantromipis.configuration.exception;

public class PropertySaveException extends RuntimeException {
    public PropertySaveException() {
    }

    public PropertySaveException(String message) {
        super(message);
    }

    public PropertySaveException(String message, Throwable cause) {
        super(message, cause);
    }

    public PropertySaveException(Throwable cause) {
        super(cause);
    }

    public PropertySaveException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
