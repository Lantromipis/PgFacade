package com.lantromipis.proxy.handler.testproxy;

import com.lantromipis.postgresprotocol.decoder.ClientPostgreSqlProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.model.StartupMessage;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import com.lantromipis.proxy.model.ProxyScramSaslAuthState;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;

public class ProxyClientHandler extends ChannelInboundHandlerAdapter {
    private Channel postgreSqlChannel;

    private final String remoteHost = "localhost";
    private final int remotePort = 5432;

    private ProxyScramSaslAuthState proxyScramSaslAuthState = ProxyScramSaslAuthState.NOT_STARTED;

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        final Channel inboundChannel = ctx.channel();

        Bootstrap b = new Bootstrap();
        b.group(inboundChannel.eventLoop())
                .channel(ctx.channel().getClass())
                .handler(new ProxyDatabaseHandler(inboundChannel))
                .option(ChannelOption.AUTO_READ, false);
        ChannelFuture f = b.connect(remoteHost, remotePort);
        postgreSqlChannel = f.channel();
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (future.isSuccess()) {
                    inboundChannel.read();
                } else {
                    inboundChannel.close();
                }
            }
        });
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        if (proxyScramSaslAuthState != ProxyScramSaslAuthState.FINISHED) {
            switch (proxyScramSaslAuthState) {
                //skip first packet as we don't need that
                case NOT_STARTED -> proxyScramSaslAuthState = ProxyScramSaslAuthState.FIRST_MESSAGE_RECEIVED;
                case FIRST_MESSAGE_RECEIVED -> {
                    StartupMessage startupMessage = ClientPostgreSqlProtocolMessageDecoder.decodeStartupMessage((ByteBuf) msg);
                    proxyScramSaslAuthState = ProxyScramSaslAuthState.STARTUP_MESSAGE_RECEIVED;
                }
                case STARTUP_MESSAGE_RECEIVED -> {
                    proxyScramSaslAuthState = ProxyScramSaslAuthState.FINISHED;
                }
            }
        }

        if (postgreSqlChannel.isActive()) {
            postgreSqlChannel.writeAndFlush(msg).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    if (future.isSuccess()) {
                        // was able to flush out data, start to read the next chunk
                        ctx.channel().read();
                    } else {
                        future.channel().close();
                    }
                }
            });
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (postgreSqlChannel != null) {
            HandlerUtils.closeOnFlush(postgreSqlChannel);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        HandlerUtils.closeOnFlush(ctx.channel());
    }
}
