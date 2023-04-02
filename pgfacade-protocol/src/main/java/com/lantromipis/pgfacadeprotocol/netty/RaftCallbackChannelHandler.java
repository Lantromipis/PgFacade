package com.lantromipis.pgfacadeprotocol.netty;

import com.lantromipis.pgfacadeprotocol.message.AbstractMessage;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftNodeCallbackInfo;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;

@Slf4j
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

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception in Raft channel! Closing channel...", cause);
        ctx.channel().close();
    }
}
