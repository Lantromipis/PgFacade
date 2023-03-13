package com.lantromipis.proxy.handler.proxy.client;

import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import com.lantromipis.proxy.handler.proxy.database.NoPoolProxyDatabaseChannelHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NoPoolProxyClientHandler extends AbstractDataProxyClientChannelHandler {
    private Channel postgreSqlChannel;

    private final String remoteHost;
    private final int remotePort;

    public NoPoolProxyClientHandler(String remoteHost, int remotePort) {
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        final Channel inboundChannel = ctx.channel();

        //TODO move away
        Bootstrap b = new Bootstrap();
        b.group(inboundChannel.eventLoop())
                .channel(ctx.channel().getClass())
                .handler(new NoPoolProxyDatabaseChannelHandler(inboundChannel))
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

        super.channelActive(ctx);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
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
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (postgreSqlChannel != null) {
            HandlerUtils.closeOnFlush(postgreSqlChannel, ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception in client connection handler. Connection will be closed ", cause);
        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void handleSwitchoverStarted() {
        forceCloseConnectionWithEmptyError();
    }

    @Override
    public void handleSwitchoverCompleted(boolean success) {
        // do nothing. Connection already closed.
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        // Overriding superclass method so channel.read() won't be called until postgresql connection is ready
    }
}
