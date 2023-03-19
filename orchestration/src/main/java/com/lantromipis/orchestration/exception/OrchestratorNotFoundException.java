package com.lantromipis.orchestration.exception;

public class OrchestratorNotFoundException extends RuntimeException {
    public OrchestratorNotFoundException() {
    }

    public OrchestratorNotFoundException(String message) {
        super(message);
    }

    public OrchestratorNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public OrchestratorNotFoundException(Throwable cause) {
        super(cause);
    }

    public OrchestratorNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
