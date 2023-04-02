package com.lantromipis.pgfacadeprotocol.utils;

import com.lantromipis.pgfacadeprotocol.message.AbstractMessage;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftNodeCallbackInfo;
import com.lantromipis.pgfacadeprotocol.netty.RaftPeerChannelInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.function.Consumer;

public class NettyUtils {

    public static void writeAndFlushIfChannelActive(Channel channel, AbstractMessage message) {
        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(message);
        }
    }

    public static ChannelFuture connectToPeerFuture(String address, int port, EventLoopGroup workerGroup, Consumer<RaftNodeCallbackInfo> responseCallback, int timoutMillis) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class)
                .option(ChannelOption.AUTO_READ, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timoutMillis)
                .handler(new RaftPeerChannelInitializer(responseCallback))
                .remoteAddress(address, port);

        return bootstrap.connect();
    }
}
