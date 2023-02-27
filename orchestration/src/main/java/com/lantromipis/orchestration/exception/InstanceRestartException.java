package com.lantromipis.orchestration.exception;

public class InstanceRestartException extends RuntimeException {
    public InstanceRestartException() {
    }

    public InstanceRestartException(String message) {
        super(message);
    }

    public InstanceRestartException(String message, Throwable cause) {
        super(message, cause);
    }

    public InstanceRestartException(Throwable cause) {
        super(cause);
    }

    public InstanceRestartException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
