package com.lantromipis.orchestration.exception;

public class SwitchoverException extends RuntimeException {
    public SwitchoverException() {
    }

    public SwitchoverException(String message) {
        super(message);
    }

    public SwitchoverException(String message, Throwable cause) {
        super(message, cause);
    }

    public SwitchoverException(Throwable cause) {
        super(cause);
    }

    public SwitchoverException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
