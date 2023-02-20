package com.lantromipis.proxy.initializer;

import com.lantromipis.proxy.producer.ProxyChannelHandlersProducer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProxyChannelInitializer extends ChannelInitializer<Channel> {
    private final ProxyChannelHandlersProducer proxyChannelHandlersProducer;

    public ProxyChannelInitializer(ProxyChannelHandlersProducer proxyChannelHandlersProducer) {
        this.proxyChannelHandlersProducer = proxyChannelHandlersProducer;
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {
        log.debug("Established new connection with client.");

        channel.pipeline().addLast(
                proxyChannelHandlersProducer.createNewClientStartupHandler()
                //new ProxyClientHandler()
        );

        if (log.isDebugEnabled()) {
            channel.pipeline().addLast(new LoggingHandler(LogLevel.DEBUG));
        }
    }
}
