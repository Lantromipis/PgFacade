package com.lantromipis.quarkusroot.producer;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Named;

@ApplicationScoped
public class EventLoopGroupProducer {
    @Produces
    @Named("worker")
    @ApplicationScoped
    public EventLoopGroup produceWorkerGroup() {
        return new NioEventLoopGroup();
    }

    @Produces
    @Named("boss")
    @ApplicationScoped
    public EventLoopGroup produceBossGroup() {
        return new NioEventLoopGroup(1);
    }
}
