package com.lantromipis.rest.model.api.shutdown;

import lombok.Data;

@Data
public class SoftShutdownRequestDto {
    private long maxClientsAwaitPeriodSeconds;
    private boolean shutdownPostgres;
    private boolean shutdownLoadBalancer;
}
