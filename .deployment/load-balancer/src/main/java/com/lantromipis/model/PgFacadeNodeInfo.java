package com.lantromipis.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PgFacadeNodeInfo {
    private String address;
    private int httpPort;
    private int primaryPort;
    private int standbyPort;
    private int maxPrimaryConnections;
    private int currentPrimaryConnections;
    private int maxStandbyConnections;
    private int currentStandbyConnections;
}
