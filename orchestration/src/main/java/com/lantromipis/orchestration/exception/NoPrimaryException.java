package com.lantromipis.orchestration.exception;

public class NoPrimaryException extends RuntimeException {
    public NoPrimaryException() {
    }

    public NoPrimaryException(String message) {
        super(message);
    }

    public NoPrimaryException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoPrimaryException(Throwable cause) {
        super(cause);
    }

    public NoPrimaryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
