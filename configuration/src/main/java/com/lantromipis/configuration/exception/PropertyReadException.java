package com.lantromipis.configuration.exception;

public class PropertyReadException extends RuntimeException {
    public PropertyReadException() {
    }

    public PropertyReadException(String message) {
        super(message);
    }

    public PropertyReadException(String message, Throwable cause) {
        super(message, cause);
    }

    public PropertyReadException(Throwable cause) {
        super(cause);
    }

    public PropertyReadException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
