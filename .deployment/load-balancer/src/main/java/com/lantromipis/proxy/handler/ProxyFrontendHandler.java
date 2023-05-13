package com.lantromipis.proxy.handler;

import com.lantromipis.utils.NettyUtils;
import io.netty.channel.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProxyFrontendHandler extends ChannelInboundHandlerAdapter {

    private final Channel outboundChannel;

    public ProxyFrontendHandler(Channel outboundChannel) {
        this.outboundChannel = outboundChannel;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        super.handlerAdded(ctx);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        outboundChannel.writeAndFlush(msg).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (future.isSuccess()) {
                    ctx.channel().read();
                } else {
                    future.channel().close();
                }
            }
        });
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (outboundChannel != null) {
            NettyUtils.closeOnFlush(outboundChannel);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error in channel fronted handler.", cause);
        NettyUtils.closeOnFlush(ctx.channel());
    }
}
