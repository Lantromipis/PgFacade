package com.lantromipis.service.impl;

import com.lantromipis.model.BalancedHostInfo;
import com.lantromipis.model.PgFacadeNodeInfo;
import com.lantromipis.properties.BalancerProperties;
import com.lantromipis.proxy.initializer.BalancedProxyInitializer;
import com.lantromipis.proxy.initializer.RoundRobinProxyChannelInitializer;
import com.lantromipis.service.api.ProxyService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@ApplicationScoped
public class ProxyServiceImpl implements ProxyService {
    @Inject
    @Named("worker")
    EventLoopGroup workerGroup;

    @Inject
    @Named("boss")
    EventLoopGroup bossGroup;

    @Inject
    BalancerProperties balancerProperties;

    @Inject
    NodeInfoProviderImpl nodeInfoProvider;

    private ChannelFuture primaryProxyBootstrapChannelFuture, standbyProxyBootstrapChannelFuture;
    private BalancedProxyInitializer primaryChannelInitializer, standbyChannelInitializer;

    private boolean active = false;

    @Override
    public void startProxy() {
        ServerBootstrap primaryProxyBootstrap = new ServerBootstrap();
        ServerBootstrap standbyProxyBootstrap = new ServerBootstrap();

        List<PgFacadeNodeInfo> nodeInfos = nodeInfoProvider.getNodeInfos();

        switch (balancerProperties.algorithm()) {
            case ROUND_ROBIN -> {
                primaryChannelInitializer = new RoundRobinProxyChannelInitializer(
                        primaryBalancerHostsFromNodesInfo(nodeInfos),
                        workerGroup
                );
                standbyChannelInitializer = new RoundRobinProxyChannelInitializer(
                        standbyBalancerHostsFromNodesInfo(nodeInfos),
                        workerGroup
                );
            }
            default ->
                    throw new IllegalStateException("No matching balancing algorithm: " + balancerProperties.algorithm());
        }

        Thread primaryBootstrapThread = new Thread(
                () -> {
                    try {
                        primaryProxyBootstrapChannelFuture = primaryProxyBootstrap.group(bossGroup, workerGroup)
                                .channel(Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                                .childHandler(primaryChannelInitializer)
                                .childOption(ChannelOption.AUTO_READ, false)
                                .bind(balancerProperties.primaryPort())
                                .sync();

                        log.info("Postgres primary proxy listening on port " + balancerProperties.primaryPort());

                        primaryProxyBootstrapChannelFuture.channel().closeFuture().sync();
                        log.info("Postgres primary proxy bootstrap stopped. Proxy not accepting connections.");

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
        );
        primaryBootstrapThread.start();

        Thread standbyBootstrapThread = new Thread(
                () -> {
                    try {
                        standbyProxyBootstrapChannelFuture = standbyProxyBootstrap.group(bossGroup, workerGroup)
                                .channel(Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                                .childHandler(standbyChannelInitializer)
                                .childOption(ChannelOption.AUTO_READ, false)
                                .bind(balancerProperties.standbyPort())
                                .sync();

                        log.info("Postgres standby proxy listening on port " + balancerProperties.standbyPort());

                        standbyProxyBootstrapChannelFuture.channel().closeFuture().sync();
                        log.info("Postgres standby proxy bootstrap stopped. Proxy not accepting connections.");

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
        );
        standbyBootstrapThread.start();

        active = true;
    }

    @Override
    public void stopProxy() {
        log.info("Stopping primary proxy...");
        primaryProxyBootstrapChannelFuture.channel().close();
        log.info("Primary proxy stopped.");
        log.info("Stopping standby proxy...");
        standbyProxyBootstrapChannelFuture.channel().close();
        log.info("Standby proxy stopped.");
    }


    @Scheduled(every = "${balancer.hosts-refresh-interval}")
    public void scheduledHostsReload() {
        if (active) {
            nodeInfoProvider.reloadHosts();
            List<PgFacadeNodeInfo> nodeInfos = nodeInfoProvider.getNodeInfos();
            primaryChannelInitializer.reloadHosts(
                    primaryBalancerHostsFromNodesInfo(nodeInfos),
                    workerGroup
            );
            standbyChannelInitializer.reloadHosts(
                    standbyBalancerHostsFromNodesInfo(nodeInfos),
                    workerGroup
            );
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("[");
            for (int i = 0; i < nodeInfos.size(); i++) {
                PgFacadeNodeInfo info = nodeInfos.get(i);

                stringBuilder.append(info.getAddress());

                if (i != nodeInfos.size() - 1) {
                    stringBuilder.append(", ");
                }
            }
            stringBuilder.append("]");
            log.info("Reloaded hosts. New hosts: {}", stringBuilder);
        }
    }

    private List<BalancedHostInfo> primaryBalancerHostsFromNodesInfo(List<PgFacadeNodeInfo> nodeInfos) {
        return nodeInfos
                .stream()
                .map(
                        info -> BalancedHostInfo
                                .builder()
                                .address(info.getAddress())
                                .port(info.getPrimaryPort())
                                .build()
                )
                .collect(Collectors.toList());
    }

    private List<BalancedHostInfo> standbyBalancerHostsFromNodesInfo(List<PgFacadeNodeInfo> nodeInfos) {
        return nodeInfos
                .stream()
                .map(
                        info -> BalancedHostInfo
                                .builder()
                                .address(info.getAddress())
                                .port(info.getStandbyPort())
                                .build()
                )
                .collect(Collectors.toList());
    }
}
