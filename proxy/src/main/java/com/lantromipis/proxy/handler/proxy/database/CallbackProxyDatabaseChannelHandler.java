package com.lantromipis.proxy.handler.proxy.database;

import com.lantromipis.postgresprotocol.utils.PostgresHandlerUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;

@Slf4j
public class CallbackProxyDatabaseChannelHandler extends ChannelInboundHandlerAdapter {

    private final Runnable connectionClosedCallback;
    private final Consumer<Object> messageReadCallback;

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        connectionClosedCallback.run();
    }

    public CallbackProxyDatabaseChannelHandler(Consumer<Object> messageReadCallback, Runnable connectionClosedCallback) {
        this.messageReadCallback = messageReadCallback;
        this.connectionClosedCallback = connectionClosedCallback;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        messageReadCallback.accept(msg);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().read();
        super.handlerAdded(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception in real Postgres connection handler. Connection will be closed and client will be disconnected.", cause);
        PostgresHandlerUtils.closeOnFlush(ctx.channel());
        connectionClosedCallback.run();
    }
}
