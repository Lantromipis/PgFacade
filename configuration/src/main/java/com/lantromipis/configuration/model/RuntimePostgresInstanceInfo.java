package com.lantromipis.configuration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RuntimePostgresInstanceInfo {
    private UUID instanceId;
    private String address;
    private int port;
    private boolean primary;
}
