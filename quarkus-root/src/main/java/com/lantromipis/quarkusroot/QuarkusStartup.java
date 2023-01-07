package com.lantromipis.quarkusroot;

import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.orchestration.orchestrator.api.PostgresOrchestrator;
import com.lantromipis.proxy.MainProxyInitializer;
import com.lantromipis.usermanagement.provider.api.UserAuthInfoProvider;
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

        postgresOrchestrator.initialize(); //TODO uncomment

        userAuthInfoProvider.initialize();
        connectionPool.initialize();

        mainProxyInitializer.initialize();

        log.info("PgFacade initialization completed!");
    }
}
