package com.lantromipis.orchestration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostgresInstanceInfo {
    private UUID instanceId;
    private String instanceAddress;
    private int instancePort;
    private InstanceStatus status;
    private InstanceHealth health;
    private Boolean master;
}
