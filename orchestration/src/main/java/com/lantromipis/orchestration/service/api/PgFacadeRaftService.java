package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.InitializationException;

public interface PgFacadeRaftService {
    void initialize() throws InitializationException;
}
