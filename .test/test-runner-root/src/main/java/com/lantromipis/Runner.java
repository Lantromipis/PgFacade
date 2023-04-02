package com.lantromipis;

import com.lantromipis.perfomance.AcquireConnectionTimeTest;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.StartupEvent;

import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.interceptor.Interceptor;

@ApplicationScoped
public class Runner {

    @Inject
    AcquireConnectionTimeTest aquireConnectionTimeTest;

    public void startup(@Observes @Priority(Interceptor.Priority.PLATFORM_BEFORE) StartupEvent startupEvent) {
        aquireConnectionTimeTest.runTest();

        Quarkus.asyncExit();
    }
}
