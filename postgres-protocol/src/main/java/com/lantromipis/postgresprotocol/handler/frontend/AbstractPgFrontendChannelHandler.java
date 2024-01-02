package com.lantromipis.postgresprotocol.handler.frontend;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;

@Slf4j
public abstract class AbstractPgFrontendChannelHandler extends ChannelInboundHandlerAdapter {

    protected ChannelHandlerContext initialChannelHandlerContext;
    protected CountDownLatch readyCountDownLatch;

    public AbstractPgFrontendChannelHandler() {
        readyCountDownLatch = null;
    }

    public AbstractPgFrontendChannelHandler(CountDownLatch readyCountDownLatch) {
        this.readyCountDownLatch = readyCountDownLatch;
    }


    public boolean isAdded() {
        return initialChannelHandlerContext != null;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        initialChannelHandlerContext = ctx;
        if (readyCountDownLatch != null) {
            readyCountDownLatch.countDown();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().read();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // only one handler can read message
    }
}
