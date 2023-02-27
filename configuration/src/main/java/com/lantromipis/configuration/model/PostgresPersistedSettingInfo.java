package com.lantromipis.configuration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostgresPersistedSettingInfo {
    private String name;
    private String value;
}
