package com.lantromipis.quarkusroot;

import com.lantromipis.configuration.event.MasterReadyEvent;
import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.orchestration.orchestrator.api.PostgresOrchestrator;
import com.lantromipis.proxy.MainProxyInitializer;
import com.lantromipis.usermanagement.provider.api.UserAuthInfoProvider;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

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

    EventLoopGroup bossGroup;
    EventLoopGroup workerGroup;

    public void startup(@Observes StartupEvent startupEvent) {
        log.info("PgFacade initialization started!");

        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        postgresOrchestrator.initialize();

        userAuthInfoProvider.initialize();
        connectionPool.initialize();

        mainProxyInitializer.initialize(bossGroup, workerGroup);

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
