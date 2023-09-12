package com.lantromipis.rest.model.api.stats;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PgFacadeSelfExternalConnectionInfoDto {
    private String address;
    private int httpPort;
    private int primaryPort;
    private int standbyPort;
}
