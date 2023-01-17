package com.lantromipis.orchestration.exception;

public class DockerEnvironmentConfigurationException extends RuntimeException {
    public DockerEnvironmentConfigurationException() {
    }

    public DockerEnvironmentConfigurationException(String message) {
        super(message);
    }

    public DockerEnvironmentConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public DockerEnvironmentConfigurationException(Throwable cause) {
        super(cause);
    }

    public DockerEnvironmentConfigurationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
