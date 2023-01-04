package com.lantromipis.exception;

public class AdapterInitializationException extends RuntimeException {
    public AdapterInitializationException() {
    }

    public AdapterInitializationException(String message) {
        super(message);
    }

    public AdapterInitializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public AdapterInitializationException(Throwable cause) {
        super(cause);
    }

    public AdapterInitializationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
