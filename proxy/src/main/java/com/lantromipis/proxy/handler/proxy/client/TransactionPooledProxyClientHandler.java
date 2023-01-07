package com.lantromipis.proxy.handler.proxy.client;

import com.lantromipis.connectionpool.model.ConnectionInfo;
import com.lantromipis.connectionpool.model.common.AuthAdditionalInfo;
import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.postgresprotocol.constant.PostgreSQLProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.model.StartupMessage;
import com.lantromipis.proxy.handler.common.AbstractProxyClientHandler;
import com.lantromipis.proxy.producer.ProxyChannelHandlersProducer;
import com.lantromipis.proxy.service.api.InactiveClientConnectionsReaper;
import io.netty.channel.Channel;

public class TransactionPooledProxyClientHandler extends AbstractProxyClientHandler {

    private final ProxyChannelHandlersProducer proxyChannelHandlersProducer;
    private final InactiveClientConnectionsReaper inactiveClientConnectionsReaper;

    private Channel masterConnection;

    private final ConnectionInfo connectionInfo;
    private final ConnectionPool connectionPool;
    private final StartupMessage startupMessage;
    private final AuthAdditionalInfo authAdditionalInfo;

    public TransactionPooledProxyClientHandler(final ConnectionPool connectionPool,
                                               final StartupMessage startupMessage,
                                               final AuthAdditionalInfo authAdditionalInfo,
                                               final ProxyChannelHandlersProducer proxyChannelHandlersProducer,
                                               final InactiveClientConnectionsReaper inactiveClientConnectionsReaper) {
        this.connectionInfo =
                ConnectionInfo
                        .builder()
                        .username(startupMessage.getParameters().get(PostgreSQLProtocolGeneralConstants.STARTUP_PARAMETER_USER))
                        .database(startupMessage.getParameters().get(PostgreSQLProtocolGeneralConstants.STARTUP_PARAMETER_DATABASE))
                        .parameters(startupMessage.getParameters())
                        .build();
        this.startupMessage = startupMessage;
        this.authAdditionalInfo = authAdditionalInfo;
        this.connectionPool = connectionPool;
        this.proxyChannelHandlersProducer = proxyChannelHandlersProducer;
        this.inactiveClientConnectionsReaper = inactiveClientConnectionsReaper;
    }
}
