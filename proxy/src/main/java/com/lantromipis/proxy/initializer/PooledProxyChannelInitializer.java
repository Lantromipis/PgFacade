package com.lantromipis.proxy.initializer;

import com.lantromipis.proxy.handler.proxy.AbstractClientChannelHandler;
import com.lantromipis.proxy.producer.ProxyChannelHandlersProducer;
import com.lantromipis.proxy.service.api.ClientConnectionsManagementService;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PooledProxyChannelInitializer extends ChannelInitializer<Channel> {

    private final ProxyChannelHandlersProducer proxyChannelHandlersProducer;
    private final ClientConnectionsManagementService clientConnectionsManagementService;

    public PooledProxyChannelInitializer(final ProxyChannelHandlersProducer proxyChannelHandlersProducer,
                                         final ClientConnectionsManagementService clientConnectionsManagementService) {
        this.proxyChannelHandlersProducer = proxyChannelHandlersProducer;
        this.clientConnectionsManagementService = clientConnectionsManagementService;
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {
        log.debug("Established new connection with client.");

        if (log.isTraceEnabled()) {
            channel.pipeline().addLast(new LoggingHandler(this.getClass(), LogLevel.TRACE));
        }

        channel.pipeline().addLast(
                proxyChannelHandlersProducer.createNewClientStartupHandler()
        );

        channel.closeFuture().addListener((ChannelFutureListener) future -> {
            AbstractClientChannelHandler clientChannelHandler = future.channel().pipeline().get(AbstractClientChannelHandler.class);
            clientConnectionsManagementService.unregisterClientChannelHandler(clientChannelHandler);
        });
    }
}
