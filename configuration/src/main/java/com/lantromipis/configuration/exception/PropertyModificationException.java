package com.lantromipis.configuration.exception;

public class PropertyModificationException extends RuntimeException {
    public PropertyModificationException() {
    }

    public PropertyModificationException(String message) {
        super(message);
    }

    public PropertyModificationException(String message, Throwable cause) {
        super(message, cause);
    }

    public PropertyModificationException(Throwable cause) {
        super(cause);
    }

    public PropertyModificationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
