package com.lantromipis.orchestration.exception;

public class PlatformAdapterOperationExecutionException extends RuntimeException {
    public PlatformAdapterOperationExecutionException() {
    }

    public PlatformAdapterOperationExecutionException(String message) {
        super(message);
    }

    public PlatformAdapterOperationExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public PlatformAdapterOperationExecutionException(Throwable cause) {
        super(cause);
    }

    public PlatformAdapterOperationExecutionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
