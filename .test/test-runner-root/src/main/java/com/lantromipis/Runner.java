package com.lantromipis;

import com.lantromipis.perfomance.AcquireConnectionTimeTest;
import com.lantromipis.perfomance.LoadTest;
import com.lantromipis.perfomance.ProxyEffectOnDelayTest;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.StartupEvent;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.interceptor.Interceptor;

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
        loadTest.runTest();
        logLine();
        aquireConnectionTimeTest.runTest();
        logLine();
        proxyEffectOnDelayTest.runTest();
        logLine();

        Quarkus.asyncExit();
    }

    private void logLine() {
        log.info("------------------------------------------------------------------------------------");
    }
}
