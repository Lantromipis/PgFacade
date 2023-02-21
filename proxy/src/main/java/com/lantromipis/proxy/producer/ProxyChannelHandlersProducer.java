package com.lantromipis.proxy.producer;

import com.lantromipis.connectionpool.model.common.AuthAdditionalInfo;
import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.postgresprotocol.model.StartupMessage;
import com.lantromipis.postgresprotocol.utils.ProtocolUtils;
import com.lantromipis.proxy.handler.general.StartupClientChannelHandler;
import com.lantromipis.proxy.handler.auth.SaslScramSha256AuthClientChannelHandler;
import com.lantromipis.proxy.handler.proxy.AbstractClientChannelHandler;
import com.lantromipis.proxy.handler.proxy.client.SessionPooledSwitchoverClosingDataProxyChannelHandler;
import com.lantromipis.proxy.handler.proxy.database.SimpleDatabaseMasterConnectionClientChannelHandler;
import com.lantromipis.proxy.service.api.ClientConnectionsManagementService;
import com.lantromipis.usermanagement.provider.api.UserAuthInfoProvider;
import io.netty.channel.Channel;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.function.Supplier;

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

    public SimpleDatabaseMasterConnectionClientChannelHandler createNewSimpleDatabaseMasterConnectionHandler(Channel clientConnection) {
        return new SimpleDatabaseMasterConnectionClientChannelHandler(clientConnection);
    }
}
