package com.lantromipis.orchestration.service.api;

public interface PostgresHealthcheckService {
    boolean isHealthyWithoutAuth(String address, int port, long timeout);
}
