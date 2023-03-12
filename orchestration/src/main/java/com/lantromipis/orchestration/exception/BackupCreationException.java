package com.lantromipis.orchestration.exception;

public class BackupCreationException extends RuntimeException {
    public BackupCreationException() {
    }

    public BackupCreationException(String message) {
        super(message);
    }

    public BackupCreationException(String message, Throwable cause) {
        super(message, cause);
    }

    public BackupCreationException(Throwable cause) {
        super(cause);
    }

    public BackupCreationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
