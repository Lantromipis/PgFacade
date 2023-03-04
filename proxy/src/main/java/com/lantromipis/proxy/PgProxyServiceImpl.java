package com.lantromipis.proxy;

import com.lantromipis.configuration.properties.predefined.ProxyProperties;
import com.lantromipis.proxy.initializer.ProxyChannelInitializer;
import com.lantromipis.proxy.producer.ProxyChannelHandlersProducer;
import com.lantromipis.proxy.service.api.ClientConnectionsManagementService;
import com.lantromipis.proxy.service.api.PgProxyService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
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

    private ChannelFuture proxyChannelFuture;

    public void initialize() {
        ServerBootstrap proxyBootstrap = new ServerBootstrap();

        Thread nettyBootstrapThread = new Thread(
                () -> {
                    try {
                        proxyChannelFuture = proxyBootstrap.group(bossGroup, workerGroup)
                                .channel(NioServerSocketChannel.class)
                                .childHandler(new ProxyChannelInitializer(proxyProperties.connectionPool().enabled(), proxyChannelHandlersProducer, clientConnectionsManagementService))
                                .childOption(ChannelOption.AUTO_READ, false)
                                .bind(proxyProperties.port())
                                .sync();

                        log.info("Postgres proxy listening on port " + proxyProperties.port());

                        proxyChannelFuture.channel().closeFuture().sync();
                        log.info("Postgres proxy bootstrap stopped. Proxy not accepting connections.");

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
        );
        nettyBootstrapThread.start();
    }

    @Override
    public void shutdown(boolean awaitClients, Duration awaitClientsDuration) {
        proxyChannelFuture.channel().close();

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
