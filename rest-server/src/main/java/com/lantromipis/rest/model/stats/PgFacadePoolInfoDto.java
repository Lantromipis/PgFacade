package com.lantromipis.rest.model.stats;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PgFacadePoolInfoDto {
    private int primaryPoolConnectionLimit;
    private int currentPrimaryPoolAllConnectionsCount;
    private int currentPrimaryPoolFreeConnectionsCount;
    private int standbyPoolConnectionLimit;
    private int currentStandbyPoolAllConnectionsCount;
    private int currentStandbyPoolFreeConnectionsCount;
}
