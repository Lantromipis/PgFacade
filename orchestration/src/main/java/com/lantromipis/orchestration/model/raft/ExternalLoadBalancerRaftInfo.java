package com.lantromipis.orchestration.model.raft;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExternalLoadBalancerRaftInfo {
    private String adapterIdentifier;
    private Instant createdWhen;
}
