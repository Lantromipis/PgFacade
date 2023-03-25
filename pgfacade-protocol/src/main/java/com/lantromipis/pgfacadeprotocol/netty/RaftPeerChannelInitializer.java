package com.lantromipis.pgfacadeprotocol.netty;

import com.lantromipis.pgfacadeprotocol.model.internal.RaftNodeCallbackInfo;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import java.util.function.Consumer;

public class RaftPeerChannelInitializer extends ChannelInitializer<SocketChannel> {

    private Consumer<RaftNodeCallbackInfo> responseCallback;

    public RaftPeerChannelInitializer(Consumer<RaftNodeCallbackInfo> responseCallback) {
        this.responseCallback = responseCallback;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(
                new RaftMessageEncoder(),
                new RaftMessageDecoder(),
                new RaftCallbackChannelHandler(responseCallback)
        );

        ch.read();

        // add channel to server

        ch.closeFuture().addListener((ChannelFutureListener) future -> {
            // remove from server
        });
    }
}
