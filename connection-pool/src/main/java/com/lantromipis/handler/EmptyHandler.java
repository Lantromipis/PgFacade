package com.lantromipis.handler;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;

@ChannelHandler.Sharable
public class EmptyHandler extends ChannelInboundHandlerAdapter {
}
