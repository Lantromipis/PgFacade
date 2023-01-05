package com.lantromipis.proxy.producer;

import com.lantromipis.connectionpool.model.common.AuthAdditionalInfo;
import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.postgresprotocol.model.StartupMessage;
import com.lantromipis.postgresprotocol.utils.ProtocolUtils;
import com.lantromipis.proxy.handler.StartupHandler;
import com.lantromipis.proxy.handler.auth.SaslScramSha256AuthHandler;
import com.lantromipis.proxy.handler.proxy.client.SessionPooledProxyClientHandler;
import com.lantromipis.proxy.handler.proxy.database.SimpleDatabaseMasterConnectionHandler;
import com.lantromipis.proxy.service.api.InactiveClientConnectionsReaper;
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
    InactiveClientConnectionsReaper inactiveClientConnectionsReaper;

    public StartupHandler createNewClientStartupHandler() {
        return new StartupHandler(userAuthInfoProvider, this);
    }

    public SaslScramSha256AuthHandler createNewSaslScramSha256AuthHandler(StartupMessage startupMessage) {
        return new SaslScramSha256AuthHandler(startupMessage, userAuthInfoProvider, this, protocolUtils);
    }

    public SessionPooledProxyClientHandler createNewSessionPooledConnectionHandler(StartupMessage startupMessage, AuthAdditionalInfo authAdditionalInfo) {
        return new SessionPooledProxyClientHandler(connectionPool, startupMessage, authAdditionalInfo, this, inactiveClientConnectionsReaper);
    }

    public SimpleDatabaseMasterConnectionHandler createNewSimpleDatabaseMasterConnectionHandler(Channel clientConnection) {
        return new SimpleDatabaseMasterConnectionHandler(clientConnection);
    }
}
