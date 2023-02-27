package com.lantromipis.orchestration.exception;

public class AwaitHealthyInstanceException extends RuntimeException {
    public AwaitHealthyInstanceException() {
    }

    public AwaitHealthyInstanceException(String message) {
        super(message);
    }

    public AwaitHealthyInstanceException(String message, Throwable cause) {
        super(message, cause);
    }

    public AwaitHealthyInstanceException(Throwable cause) {
        super(cause);
    }

    public AwaitHealthyInstanceException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
