package com.lantromipis.orchestration.exception;

public class PlatformAdapterNotFoundException extends RuntimeException {
    public PlatformAdapterNotFoundException() {
    }

    public PlatformAdapterNotFoundException(String message) {
        super(message);
    }

    public PlatformAdapterNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public PlatformAdapterNotFoundException(Throwable cause) {
        super(cause);
    }

    public PlatformAdapterNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
