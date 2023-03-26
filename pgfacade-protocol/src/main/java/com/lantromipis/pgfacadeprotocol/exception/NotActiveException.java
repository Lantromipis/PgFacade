package com.lantromipis.pgfacadeprotocol.exception;

public class NotActiveException extends RuntimeException {
    public NotActiveException() {
    }

    public NotActiveException(String message) {
        super(message);
    }

    public NotActiveException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotActiveException(Throwable cause) {
        super(cause);
    }

    public NotActiveException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
