package com.lantromipis.connectionpool.handler;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;

@ChannelHandler.Sharable
public class EmptyHandler extends ChannelInboundHandlerAdapter {
}
