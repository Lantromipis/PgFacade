package com.lantromipis.proxy.service.impl;

import com.lantromipis.configuration.properties.predefined.ProxyProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.configuration.properties.runtime.PostgresSettingsRuntimeProperties;
import com.lantromipis.postgresprotocol.producer.PgFrontendChannelHandlerProducer;
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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import lombok.extern.slf4j.Slf4j;

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

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    PgFrontendChannelHandlerProducer pgFrontendChannelHandlerProducer;

    @Inject
    PostgresSettingsRuntimeProperties postgresSettingsRuntimeProperties;

    private ChannelFuture primaryProxyChannelFuture, standbyProxyChannelFuture;

    public void initialize() {
        ServerBootstrap primaryProxyBootstrap = new ServerBootstrap();
        ServerBootstrap standbyProxyBootstrap = new ServerBootstrap();

        Thread primaryBootstrapThread = new Thread(
                () -> {
                    try {
                        ChannelHandler channelInitializer;

                        if (proxyProperties.connectionPool().enabled()) {
                            log.info("Starting primary proxy with connection pool.");
                            channelInitializer = new PooledProxyChannelInitializer(proxyChannelHandlersProducer, clientConnectionsManagementService, true);
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
                        log.error("Error while waiting for primary proxy to close.", e);
                    }
                }
        );
        Thread standbyBootstrapThread = new Thread(
                () -> {
                    try {
                        ChannelHandler channelInitializer;

                        if (proxyProperties.connectionPool().enabled()) {
                            log.info("Starting standby proxy with connection pool.");
                            channelInitializer = new PooledProxyChannelInitializer(proxyChannelHandlersProducer, clientConnectionsManagementService, false);
                        } else {
                            log.info("Starting standby proxy without connection pool.");
                            channelInitializer = new UnpooledProxyChannelInitializer(true, proxyChannelHandlersProducer, clientConnectionsManagementService);
                        }

                        standbyProxyChannelFuture = standbyProxyBootstrap.group(bossGroup, workerGroup)
                                .channel(Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                                .childHandler(channelInitializer)
                                .childOption(ChannelOption.AUTO_READ, false)
                                .bind(proxyProperties.standbyPort())
                                .sync();

                        log.info("Postgres standby proxy listening on port " + proxyProperties.standbyPort());

                        standbyProxyChannelFuture.channel().closeFuture().sync();
                        log.info("Postgres standby proxy bootstrap stopped. Proxy not accepting connections.");

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        log.error("Error while waiting for standby proxy to close.", e);
                    }
                }
        );
        primaryBootstrapThread.start();
        standbyBootstrapThread.start();
    }

    @Override
    public void shutdown(boolean awaitClients, Duration awaitClientsDuration) {
        if (primaryProxyChannelFuture == null && standbyProxyChannelFuture == null) {
            return;
        }

        if (primaryProxyChannelFuture != null) {
            primaryProxyChannelFuture.channel().close();
        }
        if (standbyProxyChannelFuture != null) {
            standbyProxyChannelFuture.channel().close();
        }

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
        primaryProxyChannelFuture = null;
        standbyProxyChannelFuture = null;
    }
}
