package com.lantromipis.orchestration.constant;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;

public class DockerConstants {

    public static final String POSTGRES_ENV_VAR_USERNAME = "POSTGRES_USER";
    public static final String POSTGRES_ENV_VAR_PASSWORD = "POSTGRES_PASSWORD";
    public static final String POSTGRES_ENV_VAR_DB = "POSTGRES_DB";

    public static final String HEALTHCHECK_CMD_SHELL = "CMD-SHELL";

    @RequiredArgsConstructor
    public enum ContainerState {
        CREATED("created"),
        RUNNING("running"),
        PAUSED("paused"),
        RESTARTING("restarting"),
        REMOVING("removing"),
        EXITED("exited"),
        DEAD("dead");

        @Getter
        private final String value;
    }

    @RequiredArgsConstructor
    public enum ContainerHealth {
        NONE("none"),
        STARTING("starting"),
        HEALTHY("healthy"),
        UNHEALTHY("unhealthy");

        @Getter
        private final String value;

        public static ContainerHealth fromValue(String value) {
            return Arrays.stream(ContainerHealth.values())
                    .filter(enm -> enm.value.equals(value))
                    .findFirst()
                    .orElse(null);
        }
    }

    private DockerConstants() {
    }
}
