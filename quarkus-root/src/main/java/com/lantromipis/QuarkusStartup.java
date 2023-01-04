package com.lantromipis;

import com.lantromipis.orchestrator.api.PostgresOrchestrator;
import com.lantromipis.pooler.api.ConnectionPool;
import com.lantromipis.provider.api.UserAuthInfoProvider;
import io.quarkus.runtime.StartupEvent;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

@Slf4j
@ApplicationScoped
public class QuarkusStartup {

    @Inject
    UserAuthInfoProvider userAuthInfoProvider;

    @Inject
    MainProxyInitializer mainProxyInitializer;

    @Inject
    ConnectionPool connectionPool;

    @Inject
    PostgresOrchestrator postgresOrchestrator;

    public void startup(@Observes StartupEvent startupEvent) {
        log.info("PgFacade initialization started!");

        postgresOrchestrator.initialize();

        userAuthInfoProvider.initialize();
        connectionPool.initialize();

        mainProxyInitializer.initialize();

        log.info("PgFacade initialization completed!");
    }
}
