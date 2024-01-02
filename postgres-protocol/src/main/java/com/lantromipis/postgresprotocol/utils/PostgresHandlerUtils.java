package com.lantromipis.postgresprotocol.utils;

import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.handler.frontend.AbstractPgFrontendChannelHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class PostgresHandlerUtils {

    public static boolean addHandlerLastAndAwaitActive(ChannelPipeline channelPipeline, AbstractPgFrontendChannelHandler abstractPgFrontendChannelHandler, long timeoutMs) {
        channelPipeline.addLast(abstractPgFrontendChannelHandler);

        long endTime = System.currentTimeMillis() + timeoutMs;
        int i = 0;

        while (!abstractPgFrontendChannelHandler.isAdded()) {
            i++;
            if (System.currentTimeMillis() > endTime) {
                log.debug("WAS WAITING FOR " + i + "TIMES");
                return false;
            }
        }
        
        return true;
    }

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

    public static void closeGracefullyOnFlush(Channel channel) {
        if (channel == null) {
            return;
        }

        try {
            closeOnFlush(channel, ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage(channel.alloc()));
        } catch (Exception e) {
            closeOnFlush(channel);
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
