package com.lantromipis.rest.model.shutdown;

import lombok.Data;

@Data
public class ForceShutdownRequestDto {
    private boolean shutdownPostgres;
    private boolean shutdownLoadBalancer;
}
