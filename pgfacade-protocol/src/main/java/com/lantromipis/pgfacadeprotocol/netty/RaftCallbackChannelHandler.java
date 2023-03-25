package com.lantromipis.pgfacadeprotocol.netty;

import com.lantromipis.pgfacadeprotocol.message.AbstractMessage;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftNodeCallbackInfo;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.function.Consumer;

public class RaftCallbackChannelHandler extends ChannelInboundHandlerAdapter {
    private Consumer<RaftNodeCallbackInfo> callbackInfoConsumer;

    public RaftCallbackChannelHandler(Consumer<RaftNodeCallbackInfo> callbackInfoConsumer) {
        this.callbackInfoConsumer = callbackInfoConsumer;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        callbackInfoConsumer.accept(RaftNodeCallbackInfo
                .builder()
                .message((AbstractMessage) msg)
                .channel(ctx.channel())
                .build()
        );
        super.channelRead(ctx, msg);
        ctx.channel().read();
    }

}
