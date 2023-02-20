package com.lantromipis.proxy.handler.proxy.database;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class SimpleDatabaseMasterConnectionClientChannelHandler extends ChannelInboundHandlerAdapter {

    private Channel clientConnection;

    public SimpleDatabaseMasterConnectionClientChannelHandler(Channel clientConnection) {
        this.clientConnection = clientConnection;
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
