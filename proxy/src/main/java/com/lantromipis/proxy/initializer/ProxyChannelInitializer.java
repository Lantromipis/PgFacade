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
public class ProxyChannelInitializer extends ChannelInitializer<Channel> {
    private final boolean connectionPoolEnabled;
    private final ProxyChannelHandlersProducer proxyChannelHandlersProducer;
    private final ClientConnectionsManagementService clientConnectionsManagementService;

    public ProxyChannelInitializer(boolean connectionPoolEnabled,
                                   final ProxyChannelHandlersProducer proxyChannelHandlersProducer,
                                   final ClientConnectionsManagementService clientConnectionsManagementService) {
        this.connectionPoolEnabled = connectionPoolEnabled;
        this.proxyChannelHandlersProducer = proxyChannelHandlersProducer;
        this.clientConnectionsManagementService = clientConnectionsManagementService;
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {
        log.debug("Established new connection with client.");

        if (log.isDebugEnabled()) {
            channel.pipeline().addLast(new LoggingHandler(LogLevel.DEBUG));
        }

        if (connectionPoolEnabled) {
            channel.pipeline().addLast(
                    proxyChannelHandlersProducer.createNewClientStartupHandler()
            );
        } else {
            channel.pipeline().addLast(
                    proxyChannelHandlersProducer.createNewNoPoolProxyClientHandler()
            );
        }

        channel.closeFuture().addListener((ChannelFutureListener) future -> {
            AbstractClientChannelHandler clientChannelHandler = future.channel().pipeline().get(AbstractClientChannelHandler.class);
            clientConnectionsManagementService.unregisterClientChannelHandler(clientChannelHandler);
        });
    }
}
