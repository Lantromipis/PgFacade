package com.lantromipis.quarkusroot;

import com.lantromipis.configuration.event.RaftLogSyncedOnStartupEvent;
import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import com.lantromipis.configuration.properties.predefined.ArchivingProperties;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.predefined.ShutdownProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.configuration.properties.runtime.PostgresSettingsRuntimeProperties;
import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.orchestration.adapter.api.ArchiverStorageAdapter;
import com.lantromipis.orchestration.adapter.api.PlatformAdaptersManager;
import com.lantromipis.orchestration.orchestrator.api.PostgresOrchestrator;
import com.lantromipis.orchestration.service.api.PgFacadeRaftService;
import com.lantromipis.proxy.service.impl.PgProxyServiceImpl;
import com.lantromipis.quarkusroot.validator.ConfigurationValidator;
import com.lantromipis.usermanagement.provider.api.UserAuthInfoProvider;
import io.netty.channel.EventLoopGroup;
import io.quarkus.arc.All;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.vertx.http.runtime.HttpConfiguration;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.interceptor.Interceptor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
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

    @Inject
    Instance<PlatformAdaptersManager> platformAdaptersManagers;

    @Inject
    Instance<ArchiverStorageAdapter> archiverStorageAdapter;

    @Inject
    ArchivingProperties archivingProperties;

    @Inject
    PgFacadeRaftService pgFacadeRaftService;

    @Inject
    OrchestrationProperties orchestrationProperties;

    @Inject
    PostgresSettingsRuntimeProperties postgresSettingsRuntimeProperties;

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    @Inject
    HttpConfiguration httpConfiguration;

    private boolean shutdownImmidiatly = false;

    public void startup(@Observes @Priority(Interceptor.Priority.PLATFORM_BEFORE) StartupEvent startupEvent) {
        try {
            log.info("Checking provided configuration values...");
            AtomicBoolean configurationValid = new AtomicBoolean(true);
            configurationValidators.forEach(configurationValidator -> {
                if (!configurationValidator.validate()) {
                    configurationValid.set(false);
                }
            });

            pgFacadeRuntimeProperties.setHttpPort(httpConfiguration.port);

            if (!configurationValid.get()) {
                log.error("CONFIGURATION INVALID. PGFACADE FAILED TO START!");
                shutdownImmediately();
                return;
            }
            log.info("Provided configuration is valid!");

            log.info("PgFacade initialization started!");

            // If no adapter, do nothing
            if (orchestrationProperties.adapter().equals(OrchestrationProperties.AdapterType.NO_ADAPTER)) {
                log.info("No orchestrator adapter is configured. PgFacade will work like proxy + connection pool without any HA features. Consider choosing another adapter if you need HA features.");
                initializeForNoAdapter();
            } else {
                if (!initializeDefault()) {
                    log.error("Failed to initialize PgFacade!");
                    shutdownImmediately();
                    return;
                }
            }

            log.info("PgFacade initialization completed!");
        } catch (Throwable t) {
            log.error("Error while starting PgFacade up!", t);
            shutdownImmediately();
        }
    }

    public void syncedWithRaftLog(@Observes @Priority(Interceptor.Priority.PLATFORM_BEFORE) RaftLogSyncedOnStartupEvent raftLogSyncedOnStartupEvent) {
        userAuthInfoProvider.initialize();
        connectionPool.initialize();
        pgProxyServiceImpl.initialize();
    }

    public void shutdownImmediately() {
        log.error("Exceptional situation occurred and it is impossible to recover! Immediately shutting down PgFacade!");
        shutdownImmidiatly = true;
        Quarkus.asyncExit(123);
    }

    public void shutdown(@Observes @Priority(Interceptor.Priority.PLATFORM_AFTER) ShutdownEvent shutdownEvent) {
        log.info("PgFacade is shutting down...");

        pgFacadeRaftService.shutdown(true);

        if (shutdownImmidiatly) {
            pgProxyServiceImpl.shutdown(false, Duration.ZERO);
        } else {
            pgProxyServiceImpl.shutdown(shutdownProperties.awaitClients(), shutdownProperties.waitForClientsDuration());
        }

        postgresOrchestrator.stopOrchestrator(false);
        platformAdaptersManagers.get().shutdown();

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

        log.info("PgFacade was shut down.");
    }

    private void initializeForNoAdapter() throws Exception {
        UUID instanceId = UUID.randomUUID();

        // Add single Postgres instance to properties
        clusterRuntimeProperties.getAllPostgresInstancesInfos().put(
                instanceId,
                RuntimePostgresInstanceInfo
                        .builder()
                        .primary(true)
                        .address(orchestrationProperties.noAdapter().primaryHost())
                        .port(orchestrationProperties.noAdapter().primaryPort())
                        .instanceId(instanceId)
                        .build()
        );

        // Set runtime properties
        postgresSettingsRuntimeProperties.reload();
        pgFacadeRuntimeProperties.setRaftRole(PgFacadeRaftRole.RAFT_DISABLED);

        userAuthInfoProvider.initialize();
        connectionPool.initialize();
        pgProxyServiceImpl.initialize();
    }

    private boolean initializeDefault() {
        // All adapters must be initialized successfully BEFORE creating Raft server
        // If initialization fails, then Raft server is not crated, so this node will never become new Leader

        // Initialize platform adapter
        try {
            platformAdaptersManagers.get().initializeAndValidate();
        } catch (Exception e) {
            log.error("Failed to initialize platform adapter!", e);
            return false;
        }

        // Initialize archiver adapter if required
        if (archivingProperties.enabled()) {
            try {
                archiverStorageAdapter.get().initializeAndValidateStorageAvailability();
            } catch (Exception e) {
                log.error("Failed to initialize archiver adapter!", e);
                return false;
            }
        } else {
            log.warn("Archiving is disabled. Continuous Archiving and Point-in-Time Recovery will not be possible!");
        }

        // All adapters initialized. Start raft server.
        // If this node becomes raft leader, then Orchestration and Archiving will be initialized in Raft state machine.
        try {
            pgFacadeRaftService.initialize();
        } catch (Exception e) {
            log.error("Failed to initialize raft service!", e);
            return false;
        }

        return true;
    }
}
