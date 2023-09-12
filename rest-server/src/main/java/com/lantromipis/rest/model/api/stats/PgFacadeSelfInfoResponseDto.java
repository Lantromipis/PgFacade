package com.lantromipis.rest.model.api.stats;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PgFacadeSelfInfoResponseDto {
    private PgFacadePoolInfoDto poolInfo;
    private PgFacadeSelfExternalConnectionInfoDto externalConnectionInfo;
}
