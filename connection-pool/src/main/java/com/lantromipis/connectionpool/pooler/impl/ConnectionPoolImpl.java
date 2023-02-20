package com.lantromipis.connectionpool.pooler.impl;

import com.lantromipis.configuration.event.SwitchoverCompletedEvent;
import com.lantromipis.configuration.event.SwitchoverStartedEvent;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.connectionpool.handler.ConnectionPoolChannelHandlerProducer;
import com.lantromipis.connectionpool.handler.EmptyHandler;
import com.lantromipis.connectionpool.model.ConnectionInfo;
import com.lantromipis.connectionpool.model.common.AuthAdditionalInfo;
import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.inject.Named;
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

    @Inject
    @Named("worker")
    EventLoopGroup workerGroup;

    private Bootstrap poolMasterBootstrap;
    //TODO capacity?
    private ConcurrentHashMap<ConnectionInfo, ConcurrentLinkedQueue<Channel>> freeConnections = new ConcurrentHashMap<>();
    private ConcurrentHashMap<ConnectionInfo, ConcurrentLinkedQueue<Channel>> allConnections = new ConcurrentHashMap<>();
    private CountDownLatch switchoverLatch = new CountDownLatch(0);

    @Override
    public void initialize() {
        createMasterBootstrap();
    }

    @Override
    //TODO retry 1 time for switchover?
    public Channel getMasterConnection(ConnectionInfo connectionInfo, AuthAdditionalInfo authAdditionalInfo) {
        if (switchoverLatch.getCount() != 0) {
            try {
                switchoverLatch.await();
            } catch (InterruptedException interruptedException) {
                log.error("Connection pool can not give connection to master. Error while waiting for switchover.", interruptedException);
                return null;
            }
        }
        ConcurrentLinkedQueue<Channel> pooledChannelsList = freeConnections.get(connectionInfo);

        if (pooledChannelsList != null) {
            Channel freeChannel = pooledChannelsList.poll();
            if (freeChannel != null) {
                log.debug("Returned connection to client from pool.");
                return freeChannel;
            }
        }

        log.debug("No connections in pool. Creating a new one...");

        ChannelFuture channelFuture = poolMasterBootstrap.connect();
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
            successLatch.await();//TODO add timeout

            if (success.get()) {
                addConnectionToMap(connectionInfo, newChannel, allConnections);
                log.debug("Successfully created new connection.");
                return newChannel;
            } else {
                newChannel.close();
                log.debug("Connection creation failed.");
                return null;
            }

        } catch (Exception e) {
            log.error("Connection pool can not give connection to master. Error while waiting for connection to be ready.", e);
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

    public void listenToSwitchoverStartedEvent(@Observes SwitchoverStartedEvent switchoverStartedEvent) {
        switchoverLatch = new CountDownLatch(1);
    }

    public void listenToSwitchoverStartedEvent(@Observes SwitchoverCompletedEvent switchoverCompletedEvent) {
        if (switchoverCompletedEvent.isSuccess()) {
            createMasterBootstrap();
            allConnections.clear();
            freeConnections.clear();
        }
        switchoverLatch.countDown();
    }

    private void createMasterBootstrap() {
        poolMasterBootstrap = new Bootstrap();
        poolMasterBootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.AUTO_READ, false)
                .handler(new EmptyHandler())
                .remoteAddress(clusterRuntimeConfiguration.getMasterHostAddress(), clusterRuntimeConfiguration.getMasterPort());
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
