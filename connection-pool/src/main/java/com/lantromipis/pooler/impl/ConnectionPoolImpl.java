package com.lantromipis.pooler.impl;

import com.lantromipis.handler.ConnectionPoolChannelHandlerProducer;
import com.lantromipis.handler.EmptyHandler;
import com.lantromipis.model.ConnectionInfo;
import com.lantromipis.model.common.AuthAdditionalInfo;
import com.lantromipis.pooler.api.ConnectionPool;
import com.lantromipis.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.utils.HandlerUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@ApplicationScoped
public class ConnectionPoolImpl implements ConnectionPool {

    @Inject
    ClusterRuntimeProperties clusterRuntimeConfiguration;

    @Inject
    ConnectionPoolChannelHandlerProducer connectionPoolChannelHandlerProducer;

    private Bootstrap poolBootstrap;
    private ConcurrentHashMap<ConnectionInfo, ConcurrentLinkedQueue<Channel>> freeConnections = new ConcurrentHashMap<>();
    private ConcurrentHashMap<ConnectionInfo, ConcurrentLinkedQueue<Channel>> allConnections = new ConcurrentHashMap<>();

    @Override
    public void initialize() {
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        poolBootstrap = new Bootstrap();
        poolBootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.AUTO_READ, false)
                .handler(new EmptyHandler())
                .remoteAddress(clusterRuntimeConfiguration.getMasterUrl(), clusterRuntimeConfiguration.getMasterPort());
    }

    @Override
    public Channel getMasterConnection(ConnectionInfo connectionInfo, AuthAdditionalInfo authAdditionalInfo) {
        ConcurrentLinkedQueue<Channel> pooledChannelsList = freeConnections.get(connectionInfo);

        if (pooledChannelsList != null) {
            Channel freeChannel = pooledChannelsList.poll();
            if (freeChannel != null) {
                log.debug("Returned connection to client from pool.");
                return freeChannel;
            }
        }

        log.debug("No connections in pool. Creating a new one...");

        ChannelFuture channelFuture = poolBootstrap.connect();
        channelFuture.awaitUninterruptibly();//TODO add timeout
        Channel newChannel = channelFuture.channel();

        try {
            newChannel.pipeline().remove(EmptyHandler.class);
        } catch (NoSuchElementException ignored) {
        }

        AtomicBoolean success = new AtomicBoolean();
        CountDownLatch successLatch = new CountDownLatch(1);

        newChannel.pipeline().addLast(
                connectionPoolChannelHandlerProducer.createNewChannelStartupHandler(
                        authAdditionalInfo,
                        connectionInfo,
                        result -> {
                            success.set(result);
                            successLatch.countDown();
                            return null;
                        }

                )
        );

        try {
            successLatch.await();

            if (success.get()) {
                addConnectionToMap(connectionInfo, newChannel, allConnections);
                log.debug("Successfully created new connection.");
                return newChannel;
            } else {
                newChannel.close();
                log.debug("Connection creation failed.");
                return null;
            }

        } catch (InterruptedException interruptedException) {
            log.error(interruptedException.getMessage(), interruptedException);
            newChannel.close();
            return null;
        }
    }

    @Override
    public void returnConnectionToPool(ConnectionInfo connectionInfo, Channel connection) {
        if (connectionInfo == null || connection == null) {
            return;
        }

        ConcurrentLinkedQueue<Channel> pooledChannelsList = allConnections.get(connectionInfo);

        if (pooledChannelsList != null && pooledChannelsList.contains(connection)) {
            HandlerUtils.removeAllHandlersFormPipeline(connection);
            addConnectionToMap(connectionInfo, connection, freeConnections);
            log.debug("Returned connection to pool.");
        }
    }

    private void addConnectionToMap(ConnectionInfo connectionInfo, Channel connection, ConcurrentHashMap<ConnectionInfo, ConcurrentLinkedQueue<Channel>> map) {
        map.compute(connectionInfo, (k, v) -> {
            if (v == null) {
                var queue = new ConcurrentLinkedQueue<Channel>();
                queue.add(connection);
                return queue;
            } else {
                v.add(connection);
                return v;
            }
        });
    }
}
