package com.lantromipis.postgresprotocol.handler.frontend;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public abstract class AbstractPgFrontendChannelHandler extends ChannelInboundHandlerAdapter {

    protected ChannelHandlerContext initialChannelHandlerContext;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        super.handlerAdded(ctx);
        initialChannelHandlerContext = ctx;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().read();
    }
}
