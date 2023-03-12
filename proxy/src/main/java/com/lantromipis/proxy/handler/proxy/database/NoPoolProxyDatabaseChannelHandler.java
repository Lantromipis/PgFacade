package com.lantromipis.proxy.handler.proxy.database;

import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import io.netty.channel.*;

public class NoPoolProxyDatabaseChannelHandler extends ChannelInboundHandlerAdapter {

    private final Channel inboundChannel;

    public NoPoolProxyDatabaseChannelHandler(Channel inboundChannel) {
        this.inboundChannel = inboundChannel;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ctx.read();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        inboundChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                ctx.channel().read();
            } else {
                future.channel().close();
            }
        });
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        HandlerUtils.closeOnFlush(inboundChannel);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        HandlerUtils.closeOnFlush(ctx.channel());
    }
}
