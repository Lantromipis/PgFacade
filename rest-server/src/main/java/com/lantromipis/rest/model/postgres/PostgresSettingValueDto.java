package com.lantromipis.rest.model.postgres;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostgresSettingValueDto {
    private String name;
    private String value;
}
