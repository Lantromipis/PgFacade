package com.lantromipis.quarkusroot;

import com.lantromipis.configuration.properties.predefined.ShutdownProperties;
import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.orchestration.service.api.PostgresOrchestrator;
import com.lantromipis.proxy.PgProxyServiceImpl;
import com.lantromipis.quarkusroot.validator.ConfigurationValidator;
import com.lantromipis.usermanagement.provider.api.UserAuthInfoProvider;
import io.netty.channel.EventLoopGroup;
import io.quarkus.arc.All;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.inject.Named;
import javax.interceptor.Interceptor;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@ApplicationScoped
public class QuarkusStartupAndShutdownHandler {

    @Inject
    UserAuthInfoProvider userAuthInfoProvider;

    @Inject
    PgProxyServiceImpl pgProxyServiceImpl;

    @Inject
    ConnectionPool connectionPool;

    @Inject
    PostgresOrchestrator postgresOrchestrator;

    @Inject
    ShutdownProperties shutdownProperties;

    @Inject
    @Named("worker")
    EventLoopGroup workerGroup;

    @Inject
    @Named("boss")
    EventLoopGroup bossGroup;

    @Inject
    @All
    List<ConfigurationValidator> configurationValidators;

    public void startup(@Observes @Priority(Interceptor.Priority.PLATFORM_BEFORE) StartupEvent startupEvent) {
        log.info("Checking provided configuration...");
        AtomicBoolean configurationValid = new AtomicBoolean(true);
        configurationValidators.forEach(configurationValidator -> {
            if (!configurationValidator.validate()) {
                configurationValid.set(false);
            }
        });

        if (!configurationValid.get()) {
            log.error("CONFIGURATION INVALID. PGFACADE WILL NOT WORK!!!");
            return;
        }
        log.info("Provided configuration is valid!");

        log.info("PgFacade initialization started!");

        postgresOrchestrator.initialize();

        userAuthInfoProvider.initialize();
        connectionPool.initialize();

        pgProxyServiceImpl.initialize();

        log.info("PgFacade initialization completed!");
    }

    public void shutdown(@Observes @Priority(Interceptor.Priority.PLATFORM_AFTER) ShutdownEvent shutdownEvent) {
        log.info("PgFacade is shutting down...");

        pgProxyServiceImpl.shutdown(shutdownProperties.awaitClients(), shutdownProperties.waitForClientsDuration());
        postgresOrchestrator.shutdown();

        try {
            bossGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }

        try {
            workerGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }

        log.info("PgFacade shut down.");
    }
}
