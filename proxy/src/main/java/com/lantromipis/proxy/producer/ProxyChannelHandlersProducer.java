package com.lantromipis.proxy.producer;

import com.lantromipis.connectionpool.model.common.AuthAdditionalInfo;
import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.postgresprotocol.model.StartupMessage;
import com.lantromipis.postgresprotocol.utils.ProtocolUtils;
import com.lantromipis.proxy.handler.general.StartupClientChannelHandler;
import com.lantromipis.proxy.handler.auth.SaslScramSha256AuthClientChannelHandler;
import com.lantromipis.proxy.handler.proxy.client.SessionPooledSwitchoverClosingDataProxyChannelHandler;
import com.lantromipis.proxy.handler.proxy.database.SimpleDatabaseMasterConnectionClientChannelHandler;
import com.lantromipis.proxy.service.api.ClientConnectionsRegistry;
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
    ClientConnectionsRegistry clientConnectionsRegistry;

    public StartupClientChannelHandler createNewClientStartupHandler() {
        return new StartupClientChannelHandler(userAuthInfoProvider, this);
    }

    public SaslScramSha256AuthClientChannelHandler createNewSaslScramSha256AuthHandler(StartupMessage startupMessage) {
        return new SaslScramSha256AuthClientChannelHandler(startupMessage, userAuthInfoProvider, this, protocolUtils);
    }

    public SessionPooledSwitchoverClosingDataProxyChannelHandler createNewSessionPooledConnectionHandler(StartupMessage startupMessage, AuthAdditionalInfo authAdditionalInfo) {
        SessionPooledSwitchoverClosingDataProxyChannelHandler ret = new SessionPooledSwitchoverClosingDataProxyChannelHandler(
                connectionPool,
                startupMessage,
                authAdditionalInfo,
                this,
                clientConnectionsRegistry
        );
        clientConnectionsRegistry.registerNewClientChannelHandler(ret);
        return ret;
    }

    public SimpleDatabaseMasterConnectionClientChannelHandler createNewSimpleDatabaseMasterConnectionHandler(Channel clientConnection) {
        return new SimpleDatabaseMasterConnectionClientChannelHandler(clientConnection);
    }
}
