package com.lantromipis.proxy.handler.proxy.client;

import com.lantromipis.connectionpool.model.PooledConnectionReturnParameters;
import com.lantromipis.connectionpool.model.PooledConnectionWrapper;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.encoder.ServerPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.internal.PgMessageInfo;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import com.lantromipis.postgresprotocol.utils.ErrorMessageUtils;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import com.lantromipis.proxy.model.FrontendConnectionState;
import com.lantromipis.proxy.producer.ProxyChannelHandlersProducer;
import com.lantromipis.proxy.service.api.ClientConnectionsManagementService;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class PooledFrontendProxyChannelHandler extends AbstractDataProxyClientChannelHandler {

    private final ProxyChannelHandlersProducer proxyChannelHandlersProducer;
    private final ClientConnectionsManagementService clientConnectionsManagementService;

    private final FrontendConnectionState frontendConnectionState;
    private final PooledConnectionWrapper primaryConnectionWrapper;

    public PooledFrontendProxyChannelHandler(final PooledConnectionWrapper primaryConnectionWrapper,
                                             final ProxyChannelHandlersProducer proxyChannelHandlersProducer,
                                             final ClientConnectionsManagementService clientConnectionsManagementService) {
        super();

        this.primaryConnectionWrapper = primaryConnectionWrapper;
        this.proxyChannelHandlersProducer = proxyChannelHandlersProducer;
        this.clientConnectionsManagementService = clientConnectionsManagementService;

        this.frontendConnectionState = FrontendConnectionState
                .builder()
                .resourcesFreed(new AtomicBoolean(false))
                .loadBalancing(false)
                .pgMessageInfos(new ArrayDeque<>())
                .build();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        if (primaryConnectionWrapper == null) {
            setInitialChannelHandlerContext(ctx);
            HandlerUtils.closeOnFlush(ctx.channel(), ErrorMessageUtils.getTooManyConnectionsErrorMessage(ctx.alloc()));
            freeResources();
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

        // intercept termination message to keep Postgres connection active
        if (DecoderUtils.checkIfMessageIsTermination(message)) {
            ctx.channel().close();
            freeResources();
            // never read again
            return;
        }

        if (!primaryConnectionWrapper.getRealPostgresConnection().isActive()) {
            super.channelRead(ctx, msg);
            return;
        }

/*        // no load-balancing
        if (!frontendConnectionState.isLoadBalancing()) {
            primaryConnectionWrapper.getRealPostgresConnection().writeAndFlush(msg)
                    .addListener((ChannelFutureListener) future -> {
                        if (future.isSuccess()) {
                            ctx.channel().read();
                        } else {
                            future.channel().close();
                        }
                    });
        } else {

        }*/

        // read message
        ByteBuf newLeftovers = DecoderUtils.splitToMessages(
                frontendConnectionState.getPrevMessageLeftovers(),
                message,
                frontendConnectionState.getPgMessageInfos(),
                ctx.alloc()
        );
        frontendConnectionState.replacePevMessageLeftovers(newLeftovers);

        PgMessageInfo pgMessageInfo = frontendConnectionState.getPgMessageInfos().poll();
        while (pgMessageInfo != null) {
            ByteBuf byteBuf = pgMessageInfo.getEntireMessage();
            byte startByte = byteBuf.readByte();

            switch (startByte) {
                case PostgresProtocolGeneralConstants.QUERY_MESSAGE_START_BYTE -> {
                    int length = byteBuf.readInt() - PostgresProtocolGeneralConstants.MESSAGE_MARKER_LENGTH_BYTES_COUNT;
                    String sqlStatement = byteBuf.toString(byteBuf.readerIndex(), length, StandardCharsets.UTF_8);
                    log.debug("SQL in simple query is {}", sqlStatement);
                }
                case PostgresProtocolGeneralConstants.PARSE_MESSAGE_START_BYTE -> {
                    int length = byteBuf.readInt() - PostgresProtocolGeneralConstants.MESSAGE_MARKER_LENGTH_BYTES_COUNT;

                    String name = DecoderUtils.readNextNullTerminatedString(byteBuf);
                    String sqlStatement = DecoderUtils.readNextNullTerminatedString(byteBuf);

                    short parametersCount = byteBuf.readShort();
                    for (short i = 0; i < parametersCount; i++) {
                        int parameterOid = byteBuf.readInt();
                    }

                    log.debug("SQL in parse is {}", sqlStatement);
                }
                case PostgresProtocolGeneralConstants.DESCRIBE_MESSAGE_START_BYTE -> {

                }
                case PostgresProtocolGeneralConstants.BIND_MESSAGE_START_BYTE -> {

                }
                case PostgresProtocolGeneralConstants.EXECUTE_MESSAGE_START_BYTE -> {

                }
                case PostgresProtocolGeneralConstants.SYNC_MESSAGE_START_BYTE -> {

                }
            }

            pgMessageInfo.getEntireMessage().release();
            pgMessageInfo = frontendConnectionState.getPgMessageInfos().poll();
        }

        primaryConnectionWrapper.getRealPostgresConnection().writeAndFlush(msg)
                .addListener((ChannelFutureListener) future -> {
                    if (future.isSuccess()) {
                        ctx.channel().read();
                    } else {
                        future.channel().close();
                    }
                });

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
        freeResources();
        ChannelHandlerContext ctx = getInitialChannelHandlerContext();
        HandlerUtils.closeOnFlush(ctx.channel(), ServerPostgresProtocolMessageEncoder.createEmptyErrorMessage(ctx.alloc()));
    }

    @Override
    public void handleSwitchoverCompleted(boolean success) {
        // do nothing. Connection with client already closed.
    }

    private void closeClientConnectionExceptionally() {
        ChannelHandlerContext ctx = getInitialChannelHandlerContext();
        HandlerUtils.closeOnFlush(ctx.channel(), ServerPostgresProtocolMessageEncoder.createEmptyErrorMessage(ctx.alloc()));
        freeResources();
    }

    private void freeResources() {
        if (frontendConnectionState.getResourcesFreed().compareAndSet(false, true)) {
            if (primaryConnectionWrapper != null) {
                primaryConnectionWrapper.returnConnectionToPool(PooledConnectionReturnParameters.builder().cleanup(true).build());
            }
            clientConnectionsManagementService.unregisterClientChannelHandler(this);
            setActive(false);
        }
    }
}
