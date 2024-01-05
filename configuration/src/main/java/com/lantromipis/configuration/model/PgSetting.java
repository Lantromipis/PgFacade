package com.lantromipis.configuration.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PgSetting {
    private String name;
    private String settingValue;
    private String context;
    private String unit;
}
