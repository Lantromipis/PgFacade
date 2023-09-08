package com.lantromipis.configuration.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum PgFacadeWorkMode {
    RECOVERY("Recovery"),
    OPERATIONAL("Operational");

    @Getter
    private final String mdcValue;
}
