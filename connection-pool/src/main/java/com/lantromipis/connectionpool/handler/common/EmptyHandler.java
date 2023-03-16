package com.lantromipis.connectionpool.handler.common;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Technical handler with no overridden behaviour
 */
@ChannelHandler.Sharable
public class EmptyHandler extends ChannelInboundHandlerAdapter {
}
