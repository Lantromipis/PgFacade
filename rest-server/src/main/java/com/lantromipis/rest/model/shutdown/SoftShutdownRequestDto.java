package com.lantromipis.rest.model.shutdown;

import lombok.Data;

@Data
public class SoftShutdownRequestDto {
    private long maxClientsAwaitPeriodMs;
    private boolean shutdownPostgres;
}
