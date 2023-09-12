package com.lantromipis.rest.model.api.postgres;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public
class PostgresSettingContextDescription {
    private String contextName;
    private boolean modifiable;
    private Boolean restartRequired;
}
