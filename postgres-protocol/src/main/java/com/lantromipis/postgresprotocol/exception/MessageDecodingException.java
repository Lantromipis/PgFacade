package com.lantromipis.postgresprotocol.exception;

public class MessageDecodingException extends RuntimeException {
    public MessageDecodingException() {
    }

    public MessageDecodingException(String message) {
        super(message);
    }

    public MessageDecodingException(String message, Throwable cause) {
        super(message, cause);
    }

    public MessageDecodingException(Throwable cause) {
        super(cause);
    }

    public MessageDecodingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
