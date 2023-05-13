package com.lantromipis;

import com.lantromipis.service.api.ProxyService;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ApplicationScoped
public class QuarkusStartupAndShutdownHandler {

    @Inject
    ProxyService proxyService;

    public void startup(@Observes @Priority(Interceptor.Priority.PLATFORM_BEFORE) StartupEvent startupEvent) {
        proxyService.startProxy();
    }

    public void shutdown(@Observes @Priority(Interceptor.Priority.PLATFORM_AFTER) ShutdownEvent shutdownEvent) {
        proxyService.stopProxy();
    }
}
