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
        if (channel == null) {
            return;
        }

        if (channel.isActive()) {
            channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }

    /**
     * Closes the specified channel after all queued write requests are flushed.
     */
    public static void closeOnFlush(Channel channel, ByteBuf message) {
        if (channel == null) {
            return;
        }

        if (channel.isActive()) {
            channel.writeAndFlush(message).addListener(ChannelFutureListener.CLOSE);
        } else {
            message.release();
        }
    }

    public static void removeAllHandlersFromChannelPipeline(Channel channel) {
        Map<String, ChannelHandler> handlers = channel.pipeline().toMap();
        handlers.forEach((key, value) -> channel.pipeline().remove(value));
    }

    public static int availableBytesInByteBuf(ByteBuf byteBuf) {
        return byteBuf.writerIndex() - byteBuf.readerIndex();
    }

    public static boolean readFromBufUntilFilled(ByteBuf dest, ByteBuf src, int requiredBytes) {
        int alreadyRead = dest.readableBytes();
        if (alreadyRead >= requiredBytes) {
            return true;
        }

        int canRead = src.readableBytes();
        int needToRead = requiredBytes - alreadyRead;
        int willRead = Math.min(needToRead, canRead);

        dest.writeBytes(src, willRead);

        return dest.readableBytes() >= requiredBytes;
    }
}
