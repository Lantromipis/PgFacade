package com.lantromipis.producer;

import com.lantromipis.handler.proxy.client.SessionPooledProxyClientHandler;
import com.lantromipis.handler.proxy.database.SimpleDatabaseMasterConnectionHandler;
import com.lantromipis.model.StartupMessage;
import com.lantromipis.model.common.AuthAdditionalInfo;
import com.lantromipis.pooler.api.ConnectionPool;
import com.lantromipis.provider.api.UserAuthInfoProvider;
import com.lantromipis.handler.StartupHandler;
import com.lantromipis.handler.auth.SaslScramSha256AuthHandler;
import com.lantromipis.service.api.InactiveClientConnectionsReaper;
import com.lantromipis.utils.ProtocolUtils;
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
