package com.lantromipis.proxy.handler.proxy.database;

import com.lantromipis.postgresprotocol.utils.PostgresHandlerUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleProxyDatabaseChannelHandler extends ChannelInboundHandlerAdapter {

    private Channel clientConnection;
    private Runnable connectionClosedCallback;

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        connectionClosedCallback.run();
    }

    public SimpleProxyDatabaseChannelHandler(Channel clientConnection, Runnable connectionClosedCallback) {
        this.clientConnection = clientConnection;
        this.connectionClosedCallback = connectionClosedCallback;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        clientConnection.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                ctx.channel().read();
            } else {
                future.channel().close();
            }
        });
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
