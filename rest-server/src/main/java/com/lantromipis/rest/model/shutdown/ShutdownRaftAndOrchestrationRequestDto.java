package com.lantromipis.rest.model.shutdown;

import lombok.Data;

@Data
public class ShutdownRaftAndOrchestrationRequestDto {
    private boolean suspend;
}
