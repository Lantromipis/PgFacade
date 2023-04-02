package com.lantromipis.orchestration.restclient.model;

import com.fasterxml.jackson.annotation.JsonValue;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;

@Data
public class HealtcheckResponseDto {
    private HealtcheckStatus status;
    private List<HealthcheckItem> checks;

    @Data
    public static class HealthcheckItem {
        private String name;
        private HealtcheckStatus status;
    }

    @RequiredArgsConstructor
    public enum HealtcheckStatus {
        UP("UP"),
        DOWN("DOWN");

        @Getter
        @JsonValue
        private final String jsonValue;
    }
}

