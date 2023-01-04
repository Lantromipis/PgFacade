package com.lantromipis.model;

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
    private String instanceIpAddress;
    private InstanceStatus status;
    private boolean master;
}
