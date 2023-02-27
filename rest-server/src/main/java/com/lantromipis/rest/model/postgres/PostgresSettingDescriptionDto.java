package com.lantromipis.rest.model.postgres;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostgresSettingDescriptionDto {
    private String name;
    private String value;
    private String unit;
    private String type;
    private String enumValues;
    private String context;
    private String description;
    private String category;
}
