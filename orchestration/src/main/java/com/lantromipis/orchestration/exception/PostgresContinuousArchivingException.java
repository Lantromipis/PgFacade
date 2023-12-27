package com.lantromipis.orchestration.exception;

public class PostgresContinuousArchivingException extends RuntimeException {
    public PostgresContinuousArchivingException() {
    }

    public PostgresContinuousArchivingException(String message) {
        super(message);
    }

    public PostgresContinuousArchivingException(String message, Throwable cause) {
        super(message, cause);
    }

    public PostgresContinuousArchivingException(Throwable cause) {
        super(cause);
    }

    public PostgresContinuousArchivingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
