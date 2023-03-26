package com.lantromipis.pgfacadeprotocol.exception;

public class NotLeaderException extends RuntimeException {
    public NotLeaderException() {
    }

    public NotLeaderException(String message) {
        super(message);
    }

    public NotLeaderException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotLeaderException(Throwable cause) {
        super(cause);
    }

    public NotLeaderException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
