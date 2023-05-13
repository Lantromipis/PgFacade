package com.lantromipis.proxy.initializer;

import com.lantromipis.model.BalancedHostInfo;
import com.lantromipis.proxy.handler.EmptyHandler;
import com.lantromipis.proxy.handler.ProxyBackendHandler;
import com.lantromipis.proxy.handler.ProxyFrontendHandler;
import com.lantromipis.utils.NettyUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
public class RoundRobinProxyChannelInitializer extends BalancedProxyInitializer {

    private final AtomicReference<RoundRobinState> roundRobinStateAtomicReference;

    public RoundRobinProxyChannelInitializer(List<BalancedHostInfo> nodes, EventLoopGroup workerGroup) {
        roundRobinStateAtomicReference = new AtomicReference<>();
        reloadHosts(nodes, workerGroup);
    }

    @Override
    public void reloadHosts(List<BalancedHostInfo> nodes, EventLoopGroup workerGroup) {
        roundRobinStateAtomicReference.set(
                RoundRobinState
                        .builder()
                        .nodes(nodes
                                .stream()
                                .map(info -> NettyUtils.createBootstrap(info.getAddress(), info.getPort(), workerGroup))
                                .collect(Collectors.toList())
                        )
                        .roundRobinAtomicInteger(new AtomicInteger(0))
                        .build()
        );
    }

    @Override
    protected void initChannel(Channel inboundChannel) throws Exception {
        RoundRobinState roundRobinState = roundRobinStateAtomicReference.get();
        ChannelFuture f = roundRobinState.getNodes().get(getNextId(roundRobinState)).connect();
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture outboundChannelFuture) throws Exception {
                if (outboundChannelFuture.isSuccess()) {
                    // connection complete start to read first data
                    outboundChannelFuture.channel().pipeline().remove(EmptyHandler.class);
                    outboundChannelFuture.channel().pipeline().addLast(
                            new ProxyBackendHandler(inboundChannel)
                    );
                    inboundChannel.pipeline().addLast(
                            new ProxyFrontendHandler(outboundChannelFuture.channel())
                    );
                    inboundChannel.read();
                } else {
                    // Close the connection if the connection attempt has failed.
                    inboundChannel.close();
                }
            }
        });
    }

    private int getNextId(RoundRobinState roundRobinState) {
        while (true) {
            int current = roundRobinState.getRoundRobinAtomicInteger().get();

            int next = current + 1;

            if (next > roundRobinState.getNodes().size()) {
                next = 0;
            }
            if (roundRobinState.getRoundRobinAtomicInteger().compareAndSet(current, next)) {
                return next;
            }
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    private static class RoundRobinState {
        private List<Bootstrap> nodes;
        private AtomicInteger roundRobinAtomicInteger;
    }
}
