package com.lantromipis.rest.model.stats;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PgFacadeNodeExternalConnectionInfoDto {
    private String ipAddress;
    private int httpPort;
    private int primaryPort;
    private int standbyPort;
}
