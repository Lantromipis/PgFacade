package com.lantromipis.restclient.model;

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
