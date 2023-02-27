package com.lantromipis.rest.exception;

public class GeneralRequestProcessingException extends RuntimeException {
    public GeneralRequestProcessingException() {
    }

    public GeneralRequestProcessingException(String message) {
        super(message);
    }

    public GeneralRequestProcessingException(String message, Throwable cause) {
        super(message, cause);
    }

    public GeneralRequestProcessingException(Throwable cause) {
        super(cause);
    }

    public GeneralRequestProcessingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
