package com.lantromipis.rest.model.api.shutdown;

import lombok.Data;

@Data
public class ForceShutdownRequestDto {
    private boolean shutdownPostgres;
    private boolean shutdownLoadBalancer;
}
