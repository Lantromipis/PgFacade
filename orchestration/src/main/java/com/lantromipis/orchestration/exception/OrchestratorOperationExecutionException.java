package com.lantromipis.orchestration.exception;

public class OrchestratorOperationExecutionException extends RuntimeException {
    public OrchestratorOperationExecutionException() {
    }

    public OrchestratorOperationExecutionException(String message) {
        super(message);
    }

    public OrchestratorOperationExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public OrchestratorOperationExecutionException(Throwable cause) {
        super(cause);
    }

    public OrchestratorOperationExecutionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
