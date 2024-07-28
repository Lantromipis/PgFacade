package com.lantromipis.orchestration.adapter.impl;

import com.lantromipis.orchestration.adapter.api.PlatformAdaptersManager;
import com.lantromipis.orchestration.exception.InitializationException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class DockerPlatformAdaptersManagerImpl implements PlatformAdaptersManager {

    @Inject
    DockerClientManager dockerClientManager;

    @Override
    public void initializeAndValidate() throws InitializationException {
        dockerClientManager.init();
    }

    @Override
    public void shutdown() {
        dockerClientManager.shutdown();
    }
}
