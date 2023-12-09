package com.lantromipis.proxy.handler.proxy.client;

import com.lantromipis.connectionpool.model.PooledConnectionReturnParameters;
import com.lantromipis.connectionpool.model.PooledConnectionWrapper;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.encoder.ServerPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import com.lantromipis.postgresprotocol.utils.ErrorMessageUtils;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import com.lantromipis.proxy.producer.ProxyChannelHandlersProducer;
import com.lantromipis.proxy.service.api.ClientConnectionsManagementService;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class SessionPooledSwitchoverClosingDataProxyChannelHandler extends AbstractDataProxyClientChannelHandler {

    private final String username;
    private final ProxyChannelHandlersProducer proxyChannelHandlersProducer;
    private final ClientConnectionsManagementService clientConnectionsManagementService;
    private PooledConnectionWrapper primaryConnectionWrapper;
    private final AtomicBoolean resourcesFreed;

    public SessionPooledSwitchoverClosingDataProxyChannelHandler(final String username,
                                                                 final PooledConnectionWrapper primaryConnectionWrapper,
                                                                 final ProxyChannelHandlersProducer proxyChannelHandlersProducer,
                                                                 final ClientConnectionsManagementService clientConnectionsManagementService) {
        this.username = username;
        this.primaryConnectionWrapper = primaryConnectionWrapper;
        this.proxyChannelHandlersProducer = proxyChannelHandlersProducer;
        this.clientConnectionsManagementService = clientConnectionsManagementService;
        resourcesFreed = new AtomicBoolean(false);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        if (primaryConnectionWrapper == null) {
            setInitialChannelHandlerContext(ctx);
            HandlerUtils.closeOnFlush(ctx.channel(), ErrorMessageUtils.getAuthFailedForUserErrorMessage(username));
            clientConnectionsManagementService.unregisterClientChannelHandler(this);
            setActive(false);
            return;
        }

        ByteBuf response = ctx.alloc().buffer(primaryConnectionWrapper.getServerParameterMessagesBytes().length + PostgresProtocolGeneralConstants.READY_FOR_QUERY_MESSAGE_LENGTH);

        response.writeBytes(primaryConnectionWrapper.getServerParameterMessagesBytes());
        response.writeBytes(ServerPostgresProtocolMessageEncoder.encodeReadyForQueryWithIdleTsxMessage());

        ctx.writeAndFlush(response);

        primaryConnectionWrapper.getRealPostgresConnection().pipeline().addLast(
                proxyChannelHandlersProducer.createNewSimpleDatabasePrimaryConnectionHandler(
                        ctx.channel(),
                        this::closeClientConnectionExceptionally
                )
        );
        // read first message from client
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

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception in client connection handler. Connection will be closed ", cause);
        closeClientConnectionExceptionally();
    }

    @Override
    public void forceDisconnectAndClearResources() {
        closeClientConnectionExceptionally();
    }

    @Override
    public void handleSwitchoverStarted() {
        resourcesFreed.set(true);
        primaryConnectionWrapper.returnConnectionToPool(
                PooledConnectionReturnParameters
                        .builder()
                        .terminate(true)
                        .build()
        );
        clientConnectionsManagementService.unregisterClientChannelHandler(this);
        setActive(false);

        HandlerUtils.closeOnFlush(getInitialChannelHandlerContext().channel(), ServerPostgresProtocolMessageEncoder.createEmptyErrorMessage());
    }

    @Override
    public void handleSwitchoverCompleted(boolean success) {
        // do nothing. Connection with client already closed.
    }

    private void closeClientConnectionExceptionally() {
        HandlerUtils.closeOnFlush(getInitialChannelHandlerContext().channel(), ServerPostgresProtocolMessageEncoder.createEmptyErrorMessage());

        freeResources();
    }

    private void closeClientConnectionSilently(ChannelHandlerContext ctx) {
        ctx.channel().close();
        freeResources();
    }

    private void freeResources() {
        if (resourcesFreed.compareAndSet(false, true)) {
            if (primaryConnectionWrapper != null) {
                primaryConnectionWrapper.returnConnectionToPool(PooledConnectionReturnParameters.builder().rollback(true).build());
            }
            clientConnectionsManagementService.unregisterClientChannelHandler(this);
            setActive(false);
        }
    }
}
