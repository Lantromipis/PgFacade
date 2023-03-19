package com.lantromipis.orchestration.model;

import lombok.*;

import java.util.Map;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostgresInstanceCreationRequest {

    private UUID futureInstanceId;

    private boolean primary;

    Map<String, String> settings;
}
