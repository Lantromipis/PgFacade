package com.lantromipis.handler.common;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;

public abstract class AbstractProxyClientHandler extends AbstractHandler {
    @Getter
    private long lastTimeAccessed;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        lastTimeAccessed = System.currentTimeMillis();
    }
}
