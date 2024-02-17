package com.lantromipis.orchestration.service.api;

public interface PostgresHealthcheckService {
    boolean checkPostgresLiveliness(String address, int port, long timeoutMs);
}
