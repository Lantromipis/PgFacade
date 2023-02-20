package com.lantromipis.quarkusroot;

import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.orchestration.service.api.PostgresOrchestrator;
import com.lantromipis.proxy.MainProxyInitializer;
import com.lantromipis.usermanagement.provider.api.UserAuthInfoProvider;
import io.netty.channel.EventLoopGroup;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.inject.Named;

@Slf4j
@ApplicationScoped
public class QuarkusStartupAndShutdownHandler {

    @Inject
    UserAuthInfoProvider userAuthInfoProvider;

    @Inject
    MainProxyInitializer mainProxyInitializer;

    @Inject
    ConnectionPool connectionPool;

    @Inject
    PostgresOrchestrator postgresOrchestrator;

    @Inject
    @Named("worker")
    EventLoopGroup workerGroup;

    @Inject
    @Named("boss")
    EventLoopGroup bossGroup;

    public void startup(@Observes StartupEvent startupEvent) {
        log.info("PgFacade initialization started!");

        postgresOrchestrator.initialize();

        userAuthInfoProvider.initialize();
        connectionPool.initialize();

        mainProxyInitializer.initialize();

        log.info("PgFacade initialization completed!");
    }

    public void shutdown(@Observes ShutdownEvent shutdownEvent) {
        log.info("PgFacade is shutting down...");

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
