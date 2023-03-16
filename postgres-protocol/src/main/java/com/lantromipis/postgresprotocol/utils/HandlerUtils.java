package com.lantromipis.postgresprotocol.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;

import java.util.Map;

public class HandlerUtils {
    /**
     * Closes the specified channel after all queued write requests are flushed.
     */
    public static void closeOnFlush(Channel channel) {
        if (channel.isActive()) {
            channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }

    /**
     * Closes the specified channel after all queued write requests are flushed.
     */
    public static void closeOnFlush(Channel channel, ByteBuf message) {
        if (channel.isActive()) {
            channel.writeAndFlush(message).addListener(ChannelFutureListener.CLOSE);
        }
    }

    public static void closeOnFlushAndAwait(Channel channel, ByteBuf message) {
        if (channel.isActive()) {
            channel.writeAndFlush(message).addListener(ChannelFutureListener.CLOSE).awaitUninterruptibly();
        }
    }

    public static void removeAllHandlersFromChannelPipeline(Channel channel) {
        Map<String, ChannelHandler> handlers = channel.pipeline().toMap();
        handlers.forEach((key, value) -> channel.pipeline().remove(value));
    }
}
