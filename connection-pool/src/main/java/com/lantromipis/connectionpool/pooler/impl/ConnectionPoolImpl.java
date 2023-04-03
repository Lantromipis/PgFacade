package com.lantromipis.connectionpool.pooler.impl;

import com.lantromipis.configuration.event.MaxConnectionsChangedEvent;
import com.lantromipis.configuration.event.SwitchoverCompletedEvent;
import com.lantromipis.configuration.event.SwitchoverStartedEvent;
import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import com.lantromipis.configuration.properties.constant.PostgresqlConfConstants;
import com.lantromipis.configuration.properties.predefined.ProxyProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.connectionpool.handler.ConnectionPoolChannelHandlerProducer;
import com.lantromipis.connectionpool.handler.common.EmptyHandler;
import com.lantromipis.connectionpool.model.*;
import com.lantromipis.connectionpool.model.auth.AuthAdditionalInfo;
import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollSocketChannel;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

    private PostgresInstancePooledConnectionsStorage primaryConnectionsStorage;
    private AtomicBoolean clearUnneededConnectionsInProgress = new AtomicBoolean(false);
    private AtomicBoolean poolActive = new AtomicBoolean(false);

    @Override
    public void initialize() {
        primaryConnectionsStorage = new PostgresInstancePooledConnectionsStorage(clusterRuntimeConfiguration.getMaxPostgresConnections());
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
        if (!poolActive.get()) {
            return null;
        }

        if (switchoverLatch.getCount() != 0) {
            try {
                if (!switchoverLatch.await(15, TimeUnit.SECONDS)) {
                    return null;
                }

                if (!poolActive.get()) {
                    return null;
                }
            } catch (InterruptedException interruptedException) {
                log.error("Connection pool can not give connection to primary. Error while waiting for switchover to complete.", interruptedException);
                Thread.currentThread().interrupt();
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

        // reserve space
        if (!storage.reserveSpaceForNewChannel()) {
            log.warn("Reached Postgres max connections limit and there are no free connections in pool. " +
                            "New connection can not be added. Consider increasing 'max_connections' Postgres setting using REST API. " +
                            "Current max_connections: {} ({} of them are reserved by PgFacade for internal needs)",
                    storage.getMaxConnections() + PostgresqlConfConstants.PG_FACADE_RESERVED_CONNECTIONS_COUNT,
                    PostgresqlConfConstants.PG_FACADE_RESERVED_CONNECTIONS_COUNT
            );

            if (proxyProperties.connectionPool().awaitConnectionWhenPoolEmpty()) {
                StorageAwaitRequest awaitRequest = new StorageAwaitRequest();
                storage.waitForConnection(awaitRequest);

                try {
                    if (awaitRequest.getCallerLatch().await(proxyProperties.connectionPool().awaitConnectionWhenPoolEmptyTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                        return wrapPooledConnection(awaitRequest.getAwaitResult(), storage);
                    } else {
                        if (!awaitRequest.getSynchronizationPoint().compareAndSet(false, true)) {
                            // unable to set! Storage returned connection after timeout!
                            return wrapPooledConnection(awaitRequest.getAwaitResult(), storage);
                        }
                    }
                } catch (Exception e) {
                    log.error("Failed while waiting for connection in pool!");
                    if (!awaitRequest.getSynchronizationPoint().compareAndSet(false, true)) {
                        // storage returned connection, but exception happened. Return back to pool.
                        Channel channel = storage.returnTakenConnectionAndCheckAge(
                                awaitRequest.getAwaitResult(),
                                proxyProperties.connectionPool().connectionMaxAge().toMillis()
                        );

                        if (channel != null) {
                            log.debug("Connection reached its max-age. Removing it from pool and closing.");
                            HandlerUtils.closeOnFlush(channel, ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage());
                        }
                    }

                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            return null;
        }

        ChannelFuture channelFuture = instanceBootstrap.connect();
        if (!channelFuture.awaitUninterruptibly(proxyProperties.connectionPool().acquireRealConnectionTimeout().toMillis())) {
            channelFuture.cancel(true);
            log.debug("Failed to acquire primary connection.");
            storage.cancelReservation();
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
                        }
                )
        );

        try {
            if (!finishedLatch.await(proxyProperties.connectionPool().realConnectionAuthTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                HandlerUtils.closeOnFlush(newChannel, ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage());
                log.debug("Failed to preform auth for new real primary connection. Timeout reached.");
                storage.cancelReservation();
                return null;
            }

            if (authResult.get() != null && authResult.get().isSuccess()) {
                ByteBuf serverParameterMessagesBuf = Unpooled.buffer(512);

                authResult.get().getServerStartMessagesInfos()
                        .stream()
                        .filter(messageInfo -> messageInfo.getStartByte() == PostgresProtocolGeneralConstants.PARAMETER_STATUS_MESSAGE_START_CHAR)
                        .forEach(messageInfo ->
                                serverParameterMessagesBuf.writeBytes(messageInfo.getEntireMessage(), 0, messageInfo.getEntireMessage().length)
                        );

                byte[] serverParameterMessagesBytes = new byte[serverParameterMessagesBuf.readableBytes()];
                serverParameterMessagesBuf.readBytes(serverParameterMessagesBytes);

                serverParameterMessagesBuf.release();

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
                storage.cancelReservation();
                return null;
            }

        } catch (Exception e) {
            log.error("Failed to preform auth for new real primary connection. Error while waiting for connection to be ready.", e);
            HandlerUtils.closeOnFlush(newChannel, ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage());
            storage.cancelReservation();
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }

    public void listenToMaxConnectionsChangedEvent(@Observes(notifyObserver = Reception.IF_EXISTS) MaxConnectionsChangedEvent maxConnectionsChangedEvent) {
        primaryConnectionsStorage.setMaxConnections(clusterRuntimeConfiguration.getMaxPostgresConnections());
    }

    public void listenToSwitchoverStartedEvent(@Observes(notifyObserver = Reception.IF_EXISTS) SwitchoverStartedEvent switchoverStartedEvent) {
        switchoverLatch = new CountDownLatch(1);
        poolActive.set(false);
    }

    public void listenToSwitchoverCompletedEvent(@Observes(notifyObserver = Reception.IF_EXISTS) SwitchoverCompletedEvent switchoverCompletedEvent) {
        if (switchoverCompletedEvent.isSuccess()) {
            primaryBootstrap = createInstanceBootstrap(clusterRuntimeConfiguration.getPrimaryInstanceInfo());
            primaryConnectionsStorage.setMaxConnections(clusterRuntimeConfiguration.getMaxPostgresConnections());
        }
        primaryConnectionsStorage.removeAllConnections().forEach(HandlerUtils::closeOnFlush);
        switchoverLatch.countDown();
        poolActive.set(switchoverCompletedEvent.isSuccess());
    }

    private PooledConnectionWrapper wrapPooledConnection(PooledConnectionInternalInfo pooledConnectionInternalInfo, PostgresInstancePooledConnectionsStorage storage) {
        return new PooledConnectionWrapper(
                pooledConnectionInternalInfo.getRealPostgresConnection(),
                pooledConnectionInternalInfo.getServerParameters(),
                params -> {
                    try {
                        if (params.isTerminate()) {
                            log.debug("Closing real Postgres connection because client connection handler requested this action.");
                            HandlerUtils.closeOnFlush(
                                    pooledConnectionInternalInfo.getRealPostgresConnection(),
                                    ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage()
                            );
                            storage.removeConnection(pooledConnectionInternalInfo);
                            return;
                        }

                        if (!pooledConnectionInternalInfo.getRealPostgresConnection().isActive()) {
                            log.debug("Tried to return connection to pool but it is already closed.");
                            return;
                        }

                        HandlerUtils.removeAllHandlersFromChannelPipeline(pooledConnectionInternalInfo.getRealPostgresConnection());

                        if (params.isRollback() && pooledConnectionInternalInfo.getRealPostgresConnection().isActive()) {
                            CountDownLatch cleanAwaitLatch = new CountDownLatch(1);
                            AtomicBoolean cleanSuccess = new AtomicBoolean(false);

                            pooledConnectionInternalInfo.getRealPostgresConnection().pipeline().addLast(
                                    connectionPoolChannelHandlerProducer.createChannelCleaningHandler(
                                            result -> {
                                                cleanAwaitLatch.countDown();
                                                cleanSuccess.set(result.isSuccess());
                                            }
                                    )
                            );

                            pooledConnectionInternalInfo.getRealPostgresConnection().read();

                            boolean awaitSuccess = cleanAwaitLatch.await(proxyProperties.connectionPool().cleanRealUsedConnectionTimeout().toMillis(), TimeUnit.MILLISECONDS);

                            if (awaitSuccess && cleanSuccess.get()) {
                                HandlerUtils.removeAllHandlersFromChannelPipeline(pooledConnectionInternalInfo.getRealPostgresConnection());
                            } else {
                                HandlerUtils.closeOnFlush(
                                        pooledConnectionInternalInfo.getRealPostgresConnection(),
                                        ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage()
                                );
                                return;
                            }
                        }

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
                    } catch (Exception e) {
                        log.error("Error while returning connection to pool", e);
                        HandlerUtils.closeOnFlush(
                                pooledConnectionInternalInfo.getRealPostgresConnection(),
                                ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage()
                        );

                        if (e instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
        );
    }

    private Bootstrap createInstanceBootstrap(RuntimePostgresInstanceInfo instanceInfo) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class)
                .option(ChannelOption.AUTO_READ, false)
                .handler(new EmptyHandler())
                .remoteAddress(
                        instanceInfo.getAddress(),
                        instanceInfo.getPort()
                );

        return bootstrap;
    }
}
