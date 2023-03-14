package com.lantromipis.connectionpool.pooler.impl;

import com.lantromipis.configuration.event.SwitchoverCompletedEvent;
import com.lantromipis.configuration.event.SwitchoverStartedEvent;
import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import com.lantromipis.configuration.properties.predefined.ProxyProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.connectionpool.handler.ConnectionPoolChannelHandlerProducer;
import com.lantromipis.connectionpool.handler.common.EmptyHandler;
import com.lantromipis.connectionpool.model.*;
import com.lantromipis.connectionpool.model.common.AuthAdditionalInfo;
import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.internal.MessageInfo;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.quarkus.scheduler.Scheduled;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.event.Reception;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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

    @Inject
    ManagedExecutor managedExecutor;

    private Bootstrap primaryBootstrap;

    private CountDownLatch switchoverLatch = new CountDownLatch(0);

    private PostgresInstancePooledConnectionsStorage primaryConnectionsStorage = new PostgresInstancePooledConnectionsStorage();
    private AtomicBoolean clearUnneededConnectionsInProgress = new AtomicBoolean(false);
    private AtomicBoolean poolActive = new AtomicBoolean(false);

    @Override
    public void initialize() {
        primaryBootstrap = createInstanceBootstrap(clusterRuntimeConfiguration.getPrimaryInstanceInfo());
        poolActive.set(true);
    }

    @Override
    public PooledConnectionWrapper getPrimaryConnection(StartupMessageInfo startupMessageInfo, AuthAdditionalInfo authAdditionalInfo) {
        return getConnection(startupMessageInfo, authAdditionalInfo, primaryBootstrap, primaryConnectionsStorage);
    }

    @Scheduled(every = "${pg-facade.proxy.connection-pool.pool-cleanup-interval}")
    public void clearUnneededConnections() {
        if (poolActive.get() && clearUnneededConnectionsInProgress.compareAndSet(false, true)) {
            try {
                long lastTimestamp = System.currentTimeMillis() - proxyProperties.connectionPool().redundantConnectionsLifetime().toMillis();
                List<Channel> redundantChannels = primaryConnectionsStorage.removeRedundantConnections(lastTimestamp, proxyProperties.connectionPool().connectionMaxAge().toMillis());
                if (CollectionUtils.isNotEmpty(redundantChannels)) {
                    redundantChannels
                            .forEach(channel -> HandlerUtils.closeOnFlush(channel, ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage()));
                    log.debug("Closed and removed from pool {} redundant connections because they were not required for too long, reached max-age or was already closed.", redundantChannels.size());
                }
            } finally {
                clearUnneededConnectionsInProgress.set(false);
            }
        }
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

        AtomicReference<PgChannelAuthResult> authResult = new AtomicReference<>();
        CountDownLatch finishedLatch = new CountDownLatch(1);

        newChannel.pipeline().addLast(
                connectionPoolChannelHandlerProducer.createNewChannelStartupHandler(
                        authAdditionalInfo,
                        startupMessageInfo,
                        result -> {
                            authResult.set(result);
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

            if (authResult.get() != null && authResult.get().isSuccess()) {
                ByteBuf serverParameterMessagesBuf = Unpooled.buffer(512);

                authResult.get().getServerStartMessagesInfos()
                        .stream()
                        .filter(messageInfo -> messageInfo.getStartByte() == PostgresProtocolGeneralConstants.PARAMETER_STATUS_MESSAGE_START_CHAR)
                        .forEach(messageInfo ->
                                serverParameterMessagesBuf.writeBytes(messageInfo.getEntireMessage(), messageInfo.getEntireMessage().readableBytes())
                        );

                byte[] serverParameterMessagesBytes = new byte[serverParameterMessagesBuf.readableBytes()];
                serverParameterMessagesBuf.readBytes(serverParameterMessagesBytes);

                pooledConnectionInternalInfo = storage.addNewChannelAndMarkAsTaken(
                        startupMessageInfo,
                        newChannel,
                        serverParameterMessagesBytes
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

    public void listenToSwitchoverStartedEvent(@Observes(notifyObserver = Reception.IF_EXISTS) SwitchoverStartedEvent switchoverStartedEvent) {
        switchoverLatch = new CountDownLatch(1);
        poolActive.set(false);
    }

    public void listenToSwitchoverCompletedEvent(@Observes(notifyObserver = Reception.IF_EXISTS) SwitchoverCompletedEvent switchoverCompletedEvent) {
        if (switchoverCompletedEvent.isSuccess()) {
            primaryBootstrap = createInstanceBootstrap(clusterRuntimeConfiguration.getPrimaryInstanceInfo());
        }
        switchoverLatch.countDown();
        poolActive.set(true);
    }

    private PooledConnectionWrapper wrapPooledConnection(PooledConnectionInternalInfo pooledConnectionInternalInfo, PostgresInstancePooledConnectionsStorage storage) {
        return new PooledConnectionWrapper(
                pooledConnectionInternalInfo.getRealPostgresConnection(),
                () -> {
                    if (!pooledConnectionInternalInfo.getRealPostgresConnection().isActive()) {
                        log.debug("Tried to return connection to pool but it is already closed. Not returning it.");
                        return;
                    }
                    HandlerUtils.removeAllHandlersFromChannelPipeline(pooledConnectionInternalInfo.getRealPostgresConnection());
                    Channel channel = storage.returnTakenConnectionAndCheckAge(
                            pooledConnectionInternalInfo,
                            proxyProperties.connectionPool().connectionMaxAge().toMillis()
                    );

                    if (channel != null) {
                        log.debug("Connection reached its max-age. Removing it from pool and closing.");
                        HandlerUtils.closeOnFlush(channel, ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage());
                    } else {
                        log.debug("Returned connection to pool.");
                    }
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
