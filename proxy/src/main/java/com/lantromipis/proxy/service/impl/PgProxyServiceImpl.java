package com.lantromipis.proxy.service.impl;

import com.lantromipis.configuration.properties.predefined.ProxyProperties;
import com.lantromipis.proxy.initializer.PooledProxyChannelInitializer;
import com.lantromipis.proxy.initializer.UnpooledProxyChannelInitializer;
import com.lantromipis.proxy.producer.ProxyChannelHandlersProducer;
import com.lantromipis.proxy.service.api.ClientConnectionsManagementService;
import com.lantromipis.proxy.service.api.PgProxyService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;
import java.time.Duration;

@Slf4j
@ApplicationScoped
public class PgProxyServiceImpl implements PgProxyService {
    @Inject
    ProxyProperties proxyProperties;

    @Inject
    ProxyChannelHandlersProducer proxyChannelHandlersProducer;

    @Inject
    ClientConnectionsManagementService clientConnectionsManagementService;

    @Inject
    @Named("worker")
    EventLoopGroup workerGroup;

    @Inject
    @Named("boss")
    EventLoopGroup bossGroup;

    private ChannelFuture primaryProxyChannelFuture;

    public void initialize() {
        ServerBootstrap primaryProxyBootstrap = new ServerBootstrap();

        Thread primaryBootstrapThread = new Thread(
                () -> {
                    try {

                        ChannelHandler channelInitializer;

                        if (proxyProperties.connectionPool().enabled()) {
                            log.info("Starting primary proxy with connection pool.");
                            channelInitializer = new PooledProxyChannelInitializer(proxyChannelHandlersProducer, clientConnectionsManagementService);
                        } else {
                            log.info("Starting primary proxy without connection pool.");
                            channelInitializer = new UnpooledProxyChannelInitializer(true, proxyChannelHandlersProducer, clientConnectionsManagementService);
                        }

                        primaryProxyChannelFuture = primaryProxyBootstrap.group(bossGroup, workerGroup)
                                .channel(Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                                .childHandler(channelInitializer)
                                .childOption(ChannelOption.AUTO_READ, false)
                                .bind(proxyProperties.primaryPort())
                                .sync();

                        log.info("Postgres primary proxy listening on port " + proxyProperties.primaryPort());

                        primaryProxyChannelFuture.channel().closeFuture().sync();
                        log.info("Postgres primary proxy bootstrap stopped. Proxy not accepting connections.");

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
        );
        primaryBootstrapThread.start();
    }

    @Override
    public void shutdown(boolean awaitClients, Duration awaitClientsDuration) {
        if (primaryProxyChannelFuture == null) {
            return;
        }

        primaryProxyChannelFuture.channel().close();

        if (!awaitClients) {
            clientConnectionsManagementService.forceDisconnectAll();
            return;
        }

        long endTime = System.currentTimeMillis() + awaitClientsDuration.toMillis();

        while (System.currentTimeMillis() < endTime) {
            if (clientConnectionsManagementService.getActiveClientsCount() == 0) {
                return;
            }
        }

        log.warn("Some clients are still connected after timeout. Force disconnecting all of them.");
        clientConnectionsManagementService.forceDisconnectAll();
    }
}
