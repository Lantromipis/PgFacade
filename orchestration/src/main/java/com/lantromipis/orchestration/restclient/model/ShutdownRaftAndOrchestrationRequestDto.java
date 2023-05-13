package com.lantromipis.orchestration.restclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShutdownRaftAndOrchestrationRequestDto {
    private boolean suspend;
}
