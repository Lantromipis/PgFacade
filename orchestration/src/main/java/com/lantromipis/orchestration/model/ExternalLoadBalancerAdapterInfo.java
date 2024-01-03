package com.lantromipis.orchestration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExternalLoadBalancerAdapterInfo {
    private boolean running;
    private String adapterIdentifier;
    private String address;
    private int httpPort;
}
