package com.lantromipis.configuration.producers;

import com.lantromipis.configuration.properties.predefined.ProxyProperties;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ApplicationScoped
public class EventLoopGroupProducer {

    @Inject
    ProxyProperties proxyProperties;

    @PostConstruct
    public void checkEpoll() {
        if (Epoll.isAvailable()) {
            log.info("Epoll available! Will use it as native transport for Netty.");
        } else {
            log.info("Epoll is not available! Will use JDK transport for Netty.");
        }
    }

    @Produces
    @Named("worker")
    @ApplicationScoped
    public EventLoopGroup produceWorkerGroup() {
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup(proxyProperties.workThreads());
        } else {
            return new NioEventLoopGroup(proxyProperties.workThreads());
        }
    }

    @Produces
    @Named("boss")
    @ApplicationScoped
    public EventLoopGroup produceBossGroup() {
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup(1);
        } else {
            return new NioEventLoopGroup(1);
        }
    }
}
