package com.lantromipis.proxy;

import com.lantromipis.configuration.properties.predefined.ProxyStaticProperties;
import com.lantromipis.proxy.initializer.ProxyChannelInitializer;
import com.lantromipis.proxy.producer.ProxyChannelHandlersProducer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;

@ApplicationScoped
@Slf4j
public class MainProxyInitializer {
    @Inject
    ProxyStaticProperties proxyProperties;

    @Inject
    ProxyChannelHandlersProducer proxyChannelHandlersProducer;

    @Inject
    @Named("worker")
    EventLoopGroup workerGroup;

    @Inject
    @Named("boss")
    EventLoopGroup bossGroup;

    public void initialize() {
        ServerBootstrap proxyBootstrap = new ServerBootstrap();

        Thread nettyBootstrapThread = new Thread(
                () -> {
                    try {
                        proxyBootstrap.group(bossGroup, workerGroup)
                                .channel(NioServerSocketChannel.class)
                                .childHandler(new ProxyChannelInitializer(proxyChannelHandlersProducer))
                                .childOption(ChannelOption.AUTO_READ, false)
                                .bind(proxyProperties.port())
                                .sync().channel().closeFuture().sync();


                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
        );
        nettyBootstrapThread.start();

        log.info("Postgres proxy listening on port " + proxyProperties.port());
    }
}
