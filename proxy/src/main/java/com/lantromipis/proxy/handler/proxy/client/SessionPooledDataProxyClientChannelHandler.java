package com.lantromipis.proxy.handler.proxy.client;

import com.lantromipis.connectionpool.model.ConnectionInfo;
import com.lantromipis.connectionpool.model.common.AuthAdditionalInfo;
import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.postgresprotocol.constant.PostgreSQLProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.encoder.ServerPostgreSqlProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.StartupMessage;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import com.lantromipis.postgresprotocol.utils.ProtocolUtils;
import com.lantromipis.proxy.producer.ProxyChannelHandlersProducer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

public class SessionPooledDataProxyClientChannelHandler extends AbstractDataProxyClientChannelHandler {

    private final ProxyChannelHandlersProducer proxyChannelHandlersProducer;

    private Channel masterConnection;

    private final ConnectionInfo connectionInfo;
    private final ConnectionPool connectionPool;
    private final AuthAdditionalInfo authAdditionalInfo;

    public SessionPooledDataProxyClientChannelHandler(final ConnectionPool connectionPool,
                                                      final StartupMessage startupMessage,
                                                      final AuthAdditionalInfo authAdditionalInfo,
                                                      final ProxyChannelHandlersProducer proxyChannelHandlersProducer) {
        super();
        this.connectionInfo =
                ConnectionInfo
                        .builder()
                        .username(startupMessage.getParameters().get(PostgreSQLProtocolGeneralConstants.STARTUP_PARAMETER_USER))
                        .database(startupMessage.getParameters().get(PostgreSQLProtocolGeneralConstants.STARTUP_PARAMETER_DATABASE))
                        .parameters(startupMessage.getParameters())
                        .build();
        this.authAdditionalInfo = authAdditionalInfo;
        this.connectionPool = connectionPool;
        this.proxyChannelHandlersProducer = proxyChannelHandlersProducer;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        masterConnection = connectionPool.getMasterConnection(connectionInfo, authAdditionalInfo);

        if (masterConnection == null) {
            rejectRequest(ctx);
        }

        masterConnection.pipeline().addLast(
                proxyChannelHandlersProducer.createNewSimpleDatabaseMasterConnectionHandler(ctx.channel())
        );
        super.handlerAdded(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;
        if (ProtocolUtils.checkIfMessageIsTermination(message)) {
            closeClientConnectionSilently(ctx);
            return;
        }

        if (masterConnection.isActive()) {
            masterConnection.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    ctx.channel().read();
                } else {
                    future.channel().close();
                }
            });
        }
        super.channelRead(ctx, msg);
    }

    private void closeClientConnectionExceptionally(ChannelHandlerContext ctx) {
        ctx.channel().writeAndFlush(ServerPostgreSqlProtocolMessageEncoder.createEmptyErrorMessage());
        HandlerUtils.closeOnFlush(ctx.channel());

        connectionPool.returnConnectionToPool(connectionInfo, masterConnection);
    }

    private void closeClientConnectionSilently(ChannelHandlerContext ctx) {
        ctx.channel().close();

        connectionPool.returnConnectionToPool(connectionInfo, masterConnection);
    }

    @Override
    public void handleInactivityPeriodEnded() {
        closeClientConnectionExceptionally(getInitialChannelHandlerContext());
    }

    @Override
    public void handleSwitchoverStarted() {

    }

    @Override
    public void handleSwitchoverCompleted(boolean success) {

    }
}
