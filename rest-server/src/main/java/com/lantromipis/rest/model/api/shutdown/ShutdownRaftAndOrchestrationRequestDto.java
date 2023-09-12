package com.lantromipis.rest.model.api.shutdown;

import lombok.Data;

@Data
public class ShutdownRaftAndOrchestrationRequestDto {
    private boolean suspend;
}
