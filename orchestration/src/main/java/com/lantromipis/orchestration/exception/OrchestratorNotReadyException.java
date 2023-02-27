package com.lantromipis.orchestration.exception;

public class OrchestratorNotReadyException extends RuntimeException {
    public OrchestratorNotReadyException() {
    }

    public OrchestratorNotReadyException(String message) {
        super(message);
    }

    public OrchestratorNotReadyException(String message, Throwable cause) {
        super(message, cause);
    }

    public OrchestratorNotReadyException(Throwable cause) {
        super(cause);
    }

    public OrchestratorNotReadyException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
