package com.lantromipis.configuration.utils;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Technical handler with no overridden behaviour
 */
@ChannelHandler.Sharable
public class EmptyNettyHandler extends ChannelInboundHandlerAdapter {
    public static final EmptyNettyHandler SHRED_EMPTY_HANDLER = new EmptyNettyHandler();
}
