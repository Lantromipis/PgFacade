package com.lantromipis;

import com.lantromipis.perfomance.AcquireConnectionTimeTest;
import com.lantromipis.perfomance.LoadTest;
import com.lantromipis.perfomance.ProxyEffectOnDelayTest;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ApplicationScoped
public class Runner {

    @Inject
    AcquireConnectionTimeTest aquireConnectionTimeTest;

    @Inject
    ProxyEffectOnDelayTest proxyEffectOnDelayTest;

    @Inject
    LoadTest loadTest;

    public void startup(@Observes @Priority(Interceptor.Priority.PLATFORM_BEFORE) StartupEvent startupEvent) {
        logLine();
        aquireConnectionTimeTest.runTest();
        logLine();
        proxyEffectOnDelayTest.runTest();
        logLine();
        loadTest.runTest();
        logLine();

        Quarkus.asyncExit();
    }

    private void logLine() {
        log.info("------------------------------------------------------------------------------------");
    }
}
