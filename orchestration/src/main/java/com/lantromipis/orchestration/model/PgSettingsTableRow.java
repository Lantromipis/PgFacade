package com.lantromipis.orchestration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PgSettingsTableRow {
    private String name;
    private String value;
    private String unit;
    private String context;
    private String vartype;
    private String enumvals;
}
