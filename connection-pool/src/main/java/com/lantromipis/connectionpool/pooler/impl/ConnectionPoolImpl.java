package com.lantromipis.connectionpool.pooler.impl;

import com.lantromipis.configuration.event.SwitchoverCompletedEvent;
import com.lantromipis.configuration.event.SwitchoverStartedEvent;
import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import com.lantromipis.configuration.properties.predefined.ProxyProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.connectionpool.handler.ConnectionPoolChannelHandlerProducer;
import com.lantromipis.connectionpool.handler.EmptyHandler;
import com.lantromipis.connectionpool.model.PooledConnectionWrapper;
import com.lantromipis.connectionpool.model.PooledConnectionInternalInfo;
import com.lantromipis.connectionpool.model.PostgresInstancePooledConnectionsStorage;
import com.lantromipis.connectionpool.model.StartupMessageInfo;
import com.lantromipis.connectionpool.model.common.AuthAdditionalInfo;
import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.NoSuchElementException;
import java.util.concurrent.*;
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

    @Inject
    ProxyProperties proxyProperties;

    private Bootstrap primaryBootstrap;

    private CountDownLatch switchoverLatch = new CountDownLatch(0);

    private PostgresInstancePooledConnectionsStorage primaryConnectionsStorage = new PostgresInstancePooledConnectionsStorage();

    @Override
    public void initialize() {
        primaryBootstrap = createInstanceBootstrap(clusterRuntimeConfiguration.getPrimaryInstanceInfo());
    }

    @Override
    public PooledConnectionWrapper getPrimaryConnection(StartupMessageInfo startupMessageInfo, AuthAdditionalInfo authAdditionalInfo) {
        return getConnection(startupMessageInfo, authAdditionalInfo, primaryBootstrap, primaryConnectionsStorage);
    }


    private PooledConnectionWrapper getConnection(StartupMessageInfo startupMessageInfo, AuthAdditionalInfo authAdditionalInfo, Bootstrap instanceBootstrap, PostgresInstancePooledConnectionsStorage storage) {
        if (switchoverLatch.getCount() != 0) {
            try {
                switchoverLatch.await();
            } catch (InterruptedException interruptedException) {
                log.error("Connection pool can not give connection to primary. Error while waiting for switchover to complete.", interruptedException);
                return null;
            }
        }

        PooledConnectionInternalInfo pooledConnectionInternalInfo = storage.getFreeConnection(startupMessageInfo);

        if (pooledConnectionInternalInfo != null) {
            log.debug("Returned primary connection to client from pool.");
            return wrapPooledConnection(
                    pooledConnectionInternalInfo,
                    storage
            );
        }

        log.debug("No free primary connections in pool. Creating a new one...");

        ChannelFuture channelFuture = instanceBootstrap.connect();
        if (!channelFuture.awaitUninterruptibly(proxyProperties.connectionPool().acquireRealConnectionTimeout().toMillis())) {
            channelFuture.cancel(true);
            log.debug("Failed to acquire primary connection.");
            return null;
        }

        Channel newChannel = channelFuture.channel();

        try {
            newChannel.pipeline().remove(EmptyHandler.class);
        } catch (NoSuchElementException ignored) {
        }

        AtomicBoolean success = new AtomicBoolean();
        CountDownLatch finishedLatch = new CountDownLatch(1);

        newChannel.pipeline().addLast(
                connectionPoolChannelHandlerProducer.createNewChannelStartupHandler(
                        authAdditionalInfo,
                        startupMessageInfo,
                        result -> {
                            success.set(result);
                            finishedLatch.countDown();
                            return null;
                        }
                )
        );

        try {
            if (!finishedLatch.await(proxyProperties.connectionPool().realConnectionAuthTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                HandlerUtils.closeOnFlush(newChannel, ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage());
                log.debug("Failed to preform auth for new real primary connection. Timeout reached.");
                return null;
            }

            if (success.get()) {
                pooledConnectionInternalInfo = storage.addNewChannelAndMarkAsTaken(
                        startupMessageInfo,
                        newChannel
                );
                log.debug("Successfully established new real primary connection. Connection added to pool and returned to client.");

                return wrapPooledConnection(
                        pooledConnectionInternalInfo,
                        storage
                );
            } else {
                HandlerUtils.closeOnFlush(newChannel);
                log.debug("Failed to preform auth for new real primary connection.");
                return null;
            }

        } catch (Exception e) {
            log.error("Failed to preform auth for new real primary connection. Error while waiting for connection to be ready.", e);
            HandlerUtils.closeOnFlush(newChannel, ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage());
            return null;
        }
    }

    public void listenToSwitchoverStartedEvent(@Observes SwitchoverStartedEvent switchoverStartedEvent) {
        switchoverLatch = new CountDownLatch(1);
    }

    public void listenToSwitchoverCompletedEvent(@Observes SwitchoverCompletedEvent switchoverCompletedEvent) {
        if (switchoverCompletedEvent.isSuccess()) {
            primaryBootstrap = createInstanceBootstrap(clusterRuntimeConfiguration.getPrimaryInstanceInfo());
        }
        switchoverLatch.countDown();
    }

    private PooledConnectionWrapper wrapPooledConnection(PooledConnectionInternalInfo pooledConnectionInternalInfo, PostgresInstancePooledConnectionsStorage storage) {
        return new PooledConnectionWrapper(
                pooledConnectionInternalInfo.getRealPostgresConnection(),
                () -> {
                    HandlerUtils.removeAllHandlersFromChannelPipeline(pooledConnectionInternalInfo.getRealPostgresConnection());
                    storage.returnTakenConnection(pooledConnectionInternalInfo);
                    log.debug("Returned connection to pool.");
                }

        );
    }

    private Bootstrap createInstanceBootstrap(RuntimePostgresInstanceInfo instanceInfo) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.AUTO_READ, false)
                .handler(new EmptyHandler())
                .remoteAddress(
                        instanceInfo.getAddress(),
                        instanceInfo.getPort()
                );

        return bootstrap;
    }
}
