package com.lantromipis.orchestration.exception;

public class RaftException extends RuntimeException {
    public RaftException() {
    }

    public RaftException(String message) {
        super(message);
    }

    public RaftException(String message, Throwable cause) {
        super(message, cause);
    }

    public RaftException(Throwable cause) {
        super(cause);
    }

    public RaftException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
