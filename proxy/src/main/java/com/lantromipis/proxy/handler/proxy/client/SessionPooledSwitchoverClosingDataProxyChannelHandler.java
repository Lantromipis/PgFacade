package com.lantromipis.proxy.handler.proxy.client;

import com.lantromipis.connectionpool.model.PooledConnectionWrapper;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.encoder.ServerPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import com.lantromipis.postgresprotocol.utils.ErrorMessageUtils;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import com.lantromipis.proxy.producer.ProxyChannelHandlersProducer;
import com.lantromipis.proxy.service.api.ClientConnectionsManagementService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SessionPooledSwitchoverClosingDataProxyChannelHandler extends AbstractDataProxyClientChannelHandler {

    private final String username;
    private final ByteBuf authOkCombined;
    private final ProxyChannelHandlersProducer proxyChannelHandlersProducer;
    private final ClientConnectionsManagementService clientConnectionsManagementService;
    private final PooledConnectionWrapper primaryConnectionWrapper;

    public SessionPooledSwitchoverClosingDataProxyChannelHandler(final String username,
                                                                 final ByteBuf authOkCombined,
                                                                 final PooledConnectionWrapper primaryConnectionWrapper,
                                                                 final ProxyChannelHandlersProducer proxyChannelHandlersProducer,
                                                                 final ClientConnectionsManagementService clientConnectionsManagementService) {
        this.username = username;
        this.authOkCombined = authOkCombined;
        this.primaryConnectionWrapper = primaryConnectionWrapper;
        this.proxyChannelHandlersProducer = proxyChannelHandlersProducer;
        this.clientConnectionsManagementService = clientConnectionsManagementService;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        if (primaryConnectionWrapper == null) {
            HandlerUtils.closeOnFlush(ctx.channel(), ErrorMessageUtils.getAuthFailedForUserErrorMessage(username));
            clientConnectionsManagementService.unregisterClientChannelHandler(this);
            setActive(false);
            return;
        }

        ByteBuf response = Unpooled.buffer(primaryConnectionWrapper.getServerParameterMessagesBytes().length + PostgresProtocolGeneralConstants.READY_FOR_QUERY_MESSAGE_LENGTH);
        response.writeBytes(authOkCombined);
        response.writeBytes(primaryConnectionWrapper.getServerParameterMessagesBytes());
        response.writeBytes(ServerPostgresProtocolMessageEncoder.encodeReadyForQueryMessage());

        ctx.writeAndFlush(response);
        
        authOkCombined.clear();

        primaryConnectionWrapper.getRealPostgresConnection().pipeline().addLast(
                proxyChannelHandlersProducer.createNewSimpleDatabasePrimaryConnectionHandler(
                        ctx.channel(),
                        () -> closeClientConnectionExceptionally(ctx)
                )
        );
        // read first message
        super.handlerAdded(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;
        if (DecoderUtils.checkIfMessageIsTermination(message)) {
            closeClientConnectionSilently(ctx);
            return;
        }

        if (primaryConnectionWrapper.getRealPostgresConnection().isActive()) {
            primaryConnectionWrapper.getRealPostgresConnection().writeAndFlush(msg)
                    .addListener((ChannelFutureListener) future -> {
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
        ctx.channel().writeAndFlush(ServerPostgresProtocolMessageEncoder.createEmptyErrorMessage());
        HandlerUtils.closeOnFlush(ctx.channel());

        primaryConnectionWrapper.returnConnectionToPool();
        clientConnectionsManagementService.unregisterClientChannelHandler(this);
        setActive(false);
    }

    private void closeClientConnectionSilently(ChannelHandlerContext ctx) {
        ctx.channel().close();

        primaryConnectionWrapper.returnConnectionToPool();
        clientConnectionsManagementService.unregisterClientChannelHandler(this);
        setActive(false);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception in client connection handler. Connection will be closed ", cause);
        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void forceDisconnect() {
        closeClientConnectionExceptionally(getInitialChannelHandlerContext());
    }

    @Override
    public void handleSwitchoverStarted() {
        closeClientConnectionExceptionally(getInitialChannelHandlerContext());
    }

    @Override
    public void handleSwitchoverCompleted(boolean success) {
        //do nothing. Connection with client already closed.
    }
}
