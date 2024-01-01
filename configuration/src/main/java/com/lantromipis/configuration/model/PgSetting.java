package com.lantromipis.configuration.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PgSetting {
    public String name;
    public String settingValue;
    public String unit;
}
