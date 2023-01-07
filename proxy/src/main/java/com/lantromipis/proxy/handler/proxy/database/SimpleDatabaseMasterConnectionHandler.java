package com.lantromipis.proxy.handler.proxy.database;

import com.lantromipis.proxy.handler.common.AbstractHandler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

public class SimpleDatabaseMasterConnectionHandler extends AbstractHandler {

    private Channel clientConnection;

    public SimpleDatabaseMasterConnectionHandler(Channel clientConnection) {
        this.clientConnection = clientConnection;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().read();
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
}