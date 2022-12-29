package com.lantromipis;

import com.lantromipis.properties.config.ProxyStaticProperties;
import com.lantromipis.producer.ProxyChannelHandlersProducer;
import com.lantromipis.initializer.ProxyChannelInitializer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
@Slf4j
public class MainProxyInitializer {
    @Inject
    ProxyStaticProperties proxyProperties;

    @Inject
    ProxyChannelHandlersProducer proxyChannelHandlersProducer;

    public void initialize() {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

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
                    } finally {
                        workerGroup.shutdownGracefully();
                        bossGroup.shutdownGracefully();
                    }
                }
        );
        nettyBootstrapThread.start();
    }
}
