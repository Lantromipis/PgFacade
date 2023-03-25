package com.lantromipis.pgfacadeprotocol.netty;

import com.lantromipis.pgfacadeprotocol.model.internal.RaftNodeCallbackInfo;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;

@Slf4j
public class RaftServerChannelInitializer extends ChannelInitializer<SocketChannel> {

    private Consumer<RaftNodeCallbackInfo> requestCallback;

    public RaftServerChannelInitializer(Consumer<RaftNodeCallbackInfo> requestCallback) {
        this.requestCallback = requestCallback;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        log.info("New raft node connection.");
        ch.pipeline().addLast(
                new RaftMessageDecoder(),
                new RaftMessageEncoder(),
                new RaftCallbackChannelHandler(requestCallback)
        );

        ch.read();
    }
}
