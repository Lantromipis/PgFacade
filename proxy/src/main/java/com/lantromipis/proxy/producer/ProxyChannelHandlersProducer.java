package com.lantromipis.proxy.producer;

import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.connectionpool.model.StartupMessageInfo;
import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.postgresprotocol.model.internal.auth.PgAuthInfo;
import com.lantromipis.postgresprotocol.model.protocol.StartupMessage;
import com.lantromipis.proxy.handler.general.StartupClientChannelHandler;
import com.lantromipis.proxy.handler.proxy.client.NoPoolProxyClientHandler;
import com.lantromipis.proxy.handler.proxy.client.PooledFrontendProxyChannelHandler;
import com.lantromipis.proxy.handler.proxy.database.CallbackProxyDatabaseChannelHandler;
import com.lantromipis.proxy.handler.proxy.database.SimpleProxyDatabaseChannelHandler;
import com.lantromipis.proxy.service.api.ClientConnectionsManagementService;
import com.lantromipis.usermanagement.provider.api.UserAuthInfoProvider;
import io.netty.channel.Channel;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.function.Consumer;

@ApplicationScoped
public class ProxyChannelHandlersProducer {
    @Inject
    UserAuthInfoProvider userAuthInfoProvider;

    @Inject
    ConnectionPool connectionPool;

    @Inject
    ClientConnectionsManagementService clientConnectionsManagementService;

    @Inject
    OrchestrationProperties orchestrationProperties;

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    public StartupClientChannelHandler createNewClientStartupHandler(boolean primaryMode) {
        StartupClientChannelHandler handler = new StartupClientChannelHandler(
                userAuthInfoProvider,
                this,
                primaryMode
        );
        clientConnectionsManagementService.registerNewClientChannelHandler(handler);
        return handler;
    }

    public void getNewSessionPooledConnectionHandlerByCallback(StartupMessage startupMessage,
                                                               PgAuthInfo pgAuthInfo,
                                                               Consumer<PooledFrontendProxyChannelHandler> createdCallback,
                                                               boolean primaryConnection
    ) {
        StartupMessageInfo startupMessageInfo = StartupMessageInfo
                .builder()
                .parameters(startupMessage.getParameters())
                .build();

        connectionPool.getPostgresConnection(
                startupMessageInfo,
                primaryConnection,
                pgAuthInfo,
                pooledConnectionWrapper -> {
                    PooledFrontendProxyChannelHandler handler = new PooledFrontendProxyChannelHandler(
                            pooledConnectionWrapper,
                            this,
                            clientConnectionsManagementService
                    );
                    createdCallback.accept(handler);
                    clientConnectionsManagementService.registerNewClientChannelHandler(handler);
                }
        );
    }

    public NoPoolProxyClientHandler createNewNoPoolProxyClientHandler(boolean primaryRequired) {
        String host;
        int port;

        if (OrchestrationProperties.AdapterType.NO_ADAPTER.equals(orchestrationProperties.adapter())) {
            host = orchestrationProperties.noAdapter().primaryHost();
            port = orchestrationProperties.noAdapter().primaryPort();
        } else {
            RuntimePostgresInstanceInfo instanceInfo;

            if (primaryRequired) {
                instanceInfo = clusterRuntimeProperties.getPrimaryInstanceInfo();
            } else {
                instanceInfo = clusterRuntimeProperties.getAllPostgresInstancesInfos()
                        .values()
                        .stream()
                        .filter(runtimePostgresInstanceInfo -> !runtimePostgresInstanceInfo.isPrimary())
                        .findFirst()
                        .orElse(null);

                if (instanceInfo == null) {
                    instanceInfo = clusterRuntimeProperties.getPrimaryInstanceInfo();
                }
            }

            host = instanceInfo.getAddress();
            port = instanceInfo.getPort();
        }

        NoPoolProxyClientHandler handler = new NoPoolProxyClientHandler(
                host,
                port
        );

        clientConnectionsManagementService.registerNewClientChannelHandler(handler);
        return handler;
    }

    public SimpleProxyDatabaseChannelHandler createNewSimpleDatabasePrimaryConnectionHandler(Channel clientConnection, Runnable realDatabaseConnectionClosedCallback) {
        return new SimpleProxyDatabaseChannelHandler(clientConnection, realDatabaseConnectionClosedCallback);
    }

    public CallbackProxyDatabaseChannelHandler createNewCallbackProxyDatabaseChannelHandler(Consumer<Object> messageReadCallback, Runnable realDatabaseConnectionClosedCallback) {
        return new CallbackProxyDatabaseChannelHandler(messageReadCallback, realDatabaseConnectionClosedCallback);
    }
}
