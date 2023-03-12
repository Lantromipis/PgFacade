package com.lantromipis.proxy.producer;

import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.connectionpool.model.common.AuthAdditionalInfo;
import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.postgresprotocol.model.StartupMessage;
import com.lantromipis.postgresprotocol.utils.ProtocolUtils;
import com.lantromipis.proxy.handler.general.StartupClientChannelHandler;
import com.lantromipis.proxy.handler.auth.SaslScramSha256AuthClientChannelHandler;
import com.lantromipis.proxy.handler.proxy.client.NoPoolProxyClientHandler;
import com.lantromipis.proxy.handler.proxy.client.SessionPooledSwitchoverClosingDataProxyChannelHandler;
import com.lantromipis.proxy.handler.proxy.database.SimpleProxyDatabaseChannelHandler;
import com.lantromipis.proxy.service.api.ClientConnectionsManagementService;
import com.lantromipis.usermanagement.provider.api.UserAuthInfoProvider;
import io.netty.channel.Channel;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class ProxyChannelHandlersProducer {
    @Inject
    UserAuthInfoProvider userAuthInfoProvider;

    @Inject
    ConnectionPool connectionPool;

    @Inject
    ProtocolUtils protocolUtils;

    @Inject
    ClientConnectionsManagementService clientConnectionsManagementService;

    @Inject
    OrchestrationProperties orchestrationProperties;

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    public StartupClientChannelHandler createNewClientStartupHandler() {
        StartupClientChannelHandler handler = new StartupClientChannelHandler(
                userAuthInfoProvider,
                this
        );
        clientConnectionsManagementService.registerNewClientChannelHandler(handler);
        return handler;
    }

    public SaslScramSha256AuthClientChannelHandler createNewSaslScramSha256AuthHandler(StartupMessage startupMessage) {
        SaslScramSha256AuthClientChannelHandler handler = new SaslScramSha256AuthClientChannelHandler(
                startupMessage,
                userAuthInfoProvider,
                this,
                protocolUtils
        );
        clientConnectionsManagementService.registerNewClientChannelHandler(handler);
        return handler;
    }

    public SessionPooledSwitchoverClosingDataProxyChannelHandler createNewSessionPooledConnectionHandler(StartupMessage startupMessage, AuthAdditionalInfo authAdditionalInfo) {
        SessionPooledSwitchoverClosingDataProxyChannelHandler handler = new SessionPooledSwitchoverClosingDataProxyChannelHandler(
                connectionPool,
                startupMessage,
                authAdditionalInfo,
                this,
                clientConnectionsManagementService
        );
        clientConnectionsManagementService.registerNewClientChannelHandler(handler);
        return handler;
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
}
