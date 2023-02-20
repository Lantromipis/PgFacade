package com.lantromipis.proxy.exception;

/**
 * Threw by ClientConnectionsRegistry if new connection handler can not be registered because of connection limit is reached.
 */
public class ConnectionLimitReachedException extends RuntimeException {
    public ConnectionLimitReachedException() {
    }

    public ConnectionLimitReachedException(String message) {
        super(message);
    }

    public ConnectionLimitReachedException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConnectionLimitReachedException(Throwable cause) {
        super(cause);
    }

    public ConnectionLimitReachedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
