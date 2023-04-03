package com.lantromipis.quarkusroot.producer;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Named;

@Slf4j
@ApplicationScoped
public class EventLoopGroupProducer {

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
            return new EpollEventLoopGroup(4);
        } else {
            return new NioEventLoopGroup(4);
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
