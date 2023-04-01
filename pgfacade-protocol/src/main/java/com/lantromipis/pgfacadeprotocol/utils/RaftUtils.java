package com.lantromipis.pgfacadeprotocol.utils;

import com.lantromipis.pgfacadeprotocol.message.AbstractMessage;
import com.lantromipis.pgfacadeprotocol.model.api.RaftServerProperties;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftNodeCallbackInfo;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftPeerWrapper;
import com.lantromipis.pgfacadeprotocol.model.internal.RaftServerContext;
import io.netty.channel.ChannelFutureListener;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Slf4j
public class RaftUtils {

    public static void updateIncrementalAtomicLong(AtomicLong atomicLong, long newValue) {
        long v;
        while (true) {
            v = atomicLong.get();

            if (v >= newValue) {
                break;
            }

            if (atomicLong.compareAndSet(v, newValue)) {
                break;
            }
            log.info("LOOPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP");
        }
    }

    public static void tryToSendMessageToAll(AbstractMessage message, RaftServerContext context, RaftServerProperties properties, Consumer<RaftNodeCallbackInfo> responseCallback) {
        for (RaftPeerWrapper wrapper : context.getRaftPeers().values()) {
            tryToSendMessageToPeer(
                    wrapper,
                    message,
                    context,
                    properties,
                    responseCallback
            );
        }
    }

    public static void tryToSendMessageToPeer(String nodeId, AbstractMessage message, RaftServerContext context, RaftServerProperties properties, Consumer<RaftNodeCallbackInfo> responseCallback) {
        tryToSendMessageToPeer(
                context.getRaftPeers().get(nodeId),
                message,
                context,
                properties,
                responseCallback
        );
    }

    public static void tryToSendMessageToPeer(RaftPeerWrapper wrapper, AbstractMessage message, RaftServerContext context, RaftServerProperties properties, Consumer<RaftNodeCallbackInfo> responseCallback) {
        if (wrapper == null) {
            return;
        }

        if (wrapper.getChannelCreatedBySelf() != null && wrapper.getChannelCreatedBySelf().isActive()) {
            wrapper.getChannelCreatedBySelf().writeAndFlush(message).addListener((ChannelFutureListener) future1 -> {
                if (future1.isSuccess()) {
                    future1.channel().read();
                }
            });

        } else {
            NettyUtils.connectToPeerFuture(
                            wrapper.getRaftNode().getIpAddress(),
                            wrapper.getRaftNode().getPort(),
                            context.getWorkerGroup(),
                            responseCallback,
                            properties.getAquireConnectionTimeout()
                    )
                    .addListener((ChannelFutureListener) future -> {
                        if (future.isSuccess()) {
                            wrapper.setChannelCreatedBySelf(future.channel());
                            future.channel().writeAndFlush(message).addListener((ChannelFutureListener) future1 -> {
                                if (future1.isSuccess()) {
                                    future1.channel().read();
                                }
                            });
                        }
                    });
        }
    }

    public static int calculateQuorum(RaftServerContext context) {
        // all peers + self
        int numberOfRaftNodes = context.getRaftPeers().size() + 1;
        return numberOfRaftNodes / 2 + 1;
    }
}
