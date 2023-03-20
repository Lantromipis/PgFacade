package com.lantromipis.orchestration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PgFacadeRaftNodeInfo {
    private String platformAdapterIdentifier;
    private String address;
    private int port;
}
