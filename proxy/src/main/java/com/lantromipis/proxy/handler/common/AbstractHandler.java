package com.lantromipis.proxy.handler.common;

import com.lantromipis.postgresprotocol.encoder.ServerPostgreSqlProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().read();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error(cause.getMessage(), cause);
        rejectRequest(ctx);
    }

    protected void rejectRequest(ChannelHandlerContext ctx) {
        ctx.channel().writeAndFlush(
                ServerPostgreSqlProtocolMessageEncoder.createEmptyErrorMessage()
        );
        HandlerUtils.closeOnFlush(ctx.channel());
    }
}
