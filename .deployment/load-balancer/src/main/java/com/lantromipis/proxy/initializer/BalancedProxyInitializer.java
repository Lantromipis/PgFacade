package com.lantromipis.proxy.initializer;

import com.lantromipis.model.BalancedHostInfo;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;

import java.util.List;

public abstract class BalancedProxyInitializer extends ChannelInitializer<Channel> {
    public abstract void reloadHosts(List<BalancedHostInfo> nodes, EventLoopGroup workerGroup) ;
}
