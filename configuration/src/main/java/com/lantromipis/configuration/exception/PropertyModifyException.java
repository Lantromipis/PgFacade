package com.lantromipis.configuration.exception;

public class PropertyModifyException extends RuntimeException {
    public PropertyModifyException() {
    }

    public PropertyModifyException(String message) {
        super(message);
    }

    public PropertyModifyException(String message, Throwable cause) {
        super(message, cause);
    }

    public PropertyModifyException(Throwable cause) {
        super(cause);
    }

    public PropertyModifyException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
