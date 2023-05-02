package com.lantromipis.orchestration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PgFacadeExternalConnectionsNodeInfo {
    private String ipAddress;
    private int httpPort;
    private int primaryPort;
    private int standbyPort;
}
