package com.lantromipis.connectionpool.pooler.impl;

import com.lantromipis.configuration.event.*;
import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import com.lantromipis.configuration.properties.constant.PostgresqlConfConstants;
import com.lantromipis.configuration.properties.predefined.ProxyProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.connectionpool.handler.ConnectionPoolChannelHandlerProducer;
import com.lantromipis.connectionpool.handler.common.EmptyHandler;
import com.lantromipis.connectionpool.model.*;
import com.lantromipis.connectionpool.model.auth.PoolAuthInfo;
import com.lantromipis.connectionpool.model.stats.ConnectionPoolStats;
import com.lantromipis.connectionpool.pooler.api.ConnectionPool;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.quarkus.scheduler.Scheduled;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.event.Reception;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

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

    private PostgresInstancePooledConnectionsStorage primaryConnectionsStorage;
    private Map<UUID, StandbyPostgresPoolWrapper> standbyConnectionsStorages = new ConcurrentHashMap<>();
    private AtomicBoolean clearUnneededConnectionsInProgress = new AtomicBoolean(false);
    private AtomicBoolean poolActive = new AtomicBoolean(false);
    private AtomicBoolean switchoverInProgress = new AtomicBoolean(false);

    @Override
    public void initialize() {
        primaryConnectionsStorage = new PostgresInstancePooledConnectionsStorage(clusterRuntimeConfiguration.getMaxPostgresConnections());
        primaryBootstrap = createInstanceBootstrap(clusterRuntimeConfiguration.getPrimaryInstanceInfo());
        poolActive.set(true);
    }

    @Override
    public void getPostgresConnection(StartupMessageInfo startupMessageInfo, boolean primary, PoolAuthInfo poolAuthInfo, Consumer<PooledConnectionWrapper> readyCallback) {
        if (primary) {
            log.debug("Primary connection requested.");
            getConnection(startupMessageInfo, poolAuthInfo, primaryBootstrap, primaryConnectionsStorage, readyCallback);
        } else {
            log.debug("Standby connection requested.");
            StandbyPostgresPoolWrapper wrapper = getLeastLoadedStandbyStorage();
            if (wrapper != null) {
                getConnection(startupMessageInfo, poolAuthInfo, wrapper.getStandbyBootstrap(), wrapper.getStorage(), readyCallback);
            } else {
                readyCallback.accept(null);
            }
        }
    }

    @Override
    public ConnectionPoolStats getStats() {
        int standbyPoolAllConnectionsCount = -1;
        int standbyPoolConnectionsLimit = -1;
        int standbyPoolFreeConnectionsLimit = -1;

        if (MapUtils.isNotEmpty(standbyConnectionsStorages)) {
            standbyPoolAllConnectionsCount = standbyConnectionsStorages.values()
                    .stream()
                    .mapToInt(w -> w.getStorage().getAllConnectionsCount())
                    .sum();

            standbyPoolConnectionsLimit = standbyConnectionsStorages.values()
                    .stream()
                    .mapToInt(w -> w.getStorage().getMaxConnections())
                    .sum();

            standbyPoolFreeConnectionsLimit = standbyConnectionsStorages.values()
                    .stream()
                    .mapToInt(w -> w.getStorage().getFreeConnectionsCount())
                    .sum();
        }

        return ConnectionPoolStats
                .builder()
                .primaryPoolAllConnectionsCount(primaryConnectionsStorage == null ? -1 : primaryConnectionsStorage.getAllConnectionsCount())
                .standbyPoolAllConnectionsCount(standbyPoolAllConnectionsCount)
                .primaryPoolFreeConnectionsCount(primaryConnectionsStorage == null ? -1 : primaryConnectionsStorage.getFreeConnectionsCount())
                .standbyPoolFreeConnectionsCount(standbyPoolFreeConnectionsLimit)
                .primaryPoolConnectionsLimit(primaryConnectionsStorage == null ? -1 : primaryConnectionsStorage.getMaxConnections())
                .standbyPoolConnectionsLimit(standbyPoolConnectionsLimit)
                .build();
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

    private void getConnection(StartupMessageInfo startupMessageInfo, PoolAuthInfo poolAuthInfo, Bootstrap instanceBootstrap, PostgresInstancePooledConnectionsStorage storage, Consumer<PooledConnectionWrapper> readyCallback) {
        if (!poolActive.get()) {
            readyCallback.accept(null);
            return;
        }

        if (switchoverInProgress.get()) {
            // TODO add scheduler and cancel
            readyCallback.accept(null);
            return;
        }

        PooledConnectionInternalInfo pooledConnectionInternalInfo = storage.getFreeConnection(startupMessageInfo);

        if (pooledConnectionInternalInfo != null) {
            log.debug("Returned primary connection to client from pool.");
            readyCallback.accept(
                    wrapPooledConnection(
                            pooledConnectionInternalInfo,
                            storage
                    )
            );
            return;
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
                        readyCallback.accept(wrapPooledConnection(awaitRequest.getAwaitResult(), storage));
                        return;
                    } else {
                        if (!awaitRequest.getSynchronizationPoint().compareAndSet(false, true)) {
                            // unable to set! Storage returned connection after timeout!
                            readyCallback.accept(wrapPooledConnection(awaitRequest.getAwaitResult(), storage));
                            return;
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

            readyCallback.accept(null);
            return;
        }

        ChannelFuture channelFuture = instanceBootstrap.connect();

        channelFuture.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                Channel channel = future.channel();

                channel.pipeline().remove(EmptyHandler.class);
                AtomicBoolean finished = new AtomicBoolean(false);

                ScheduledFuture<?> cancelFuture = channel.eventLoop().schedule(() -> {
                            if (finished.compareAndSet(false, true)) {
                                readyCallback.accept(null);
                                HandlerUtils.closeOnFlush(channel);
                                log.warn("Timeout reached for real Postgres connection auth.");
                                storage.cancelReservation();
                            } else {
                                log.debug("Postgres connection acquired. Canceling scheduled...");
                            }
                        },
                        proxyProperties.connectionPool().realConnectionAuthTimeout().toMillis(),
                        TimeUnit.MILLISECONDS
                );

                channel.pipeline().addLast(
                        connectionPoolChannelHandlerProducer.createNewChannelStartupHandler(
                                poolAuthInfo,
                                startupMessageInfo,
                                result -> {
                                    if (finished.compareAndSet(false, true)) {
                                        cancelFuture.cancel(false);
                                    } else {
                                        return;
                                    }

                                    if (result.isSuccess()) {
                                        ByteBuf serverParameterMessagesBuf = Unpooled.buffer(512);

                                        result.getServerStartMessagesInfos()
                                                .stream()
                                                .filter(messageInfo -> messageInfo.getStartByte() == PostgresProtocolGeneralConstants.PARAMETER_STATUS_MESSAGE_START_CHAR)
                                                .forEach(messageInfo ->
                                                        serverParameterMessagesBuf.writeBytes(messageInfo.getEntireMessage(), 0, messageInfo.getEntireMessage().length)
                                                );

                                        byte[] serverParameterMessagesBytes = new byte[serverParameterMessagesBuf.readableBytes()];
                                        serverParameterMessagesBuf.readBytes(serverParameterMessagesBytes);

                                        serverParameterMessagesBuf.release();

                                        PooledConnectionInternalInfo pooledConnectionInternalInfo1 = storage.addNewChannelAndMarkAsTaken(
                                                startupMessageInfo,
                                                channel,
                                                serverParameterMessagesBytes
                                        );
                                        log.debug("Successfully established new real postgres connection. Connection added to pool and returned to client.");

                                        readyCallback.accept(
                                                wrapPooledConnection(
                                                        pooledConnectionInternalInfo1,
                                                        storage
                                                )
                                        );
                                    } else {
                                        readyCallback.accept(null);
                                        HandlerUtils.closeOnFlush(channel);
                                        log.debug("Failed to preform auth for new real postgres connection.");
                                        storage.cancelReservation();
                                    }
                                }
                        )
                );
            } else {
                log.debug("Failed to acquire real postgres connection.");
                storage.cancelReservation();
                readyCallback.accept(null);
            }
        });
    }

    public void listenToMaxConnectionsChangedEvent(@Observes(notifyObserver = Reception.IF_EXISTS) MaxConnectionsChangedEvent maxConnectionsChangedEvent) {
        if (primaryConnectionsStorage == null) {
            return;
        }
        primaryConnectionsStorage.setMaxConnections(clusterRuntimeConfiguration.getMaxPostgresConnections());
    }

    public void listenToSwitchoverStartedEvent(@Observes(notifyObserver = Reception.IF_EXISTS) SwitchoverStartedEvent switchoverStartedEvent) {
        switchoverInProgress.set(true);
        poolActive.set(false);
    }

    public void listenToSwitchoverCompletedEvent(@Observes(notifyObserver = Reception.IF_EXISTS) SwitchoverCompletedEvent switchoverCompletedEvent) {
        if (switchoverCompletedEvent.isSuccess()) {
            primaryBootstrap = createInstanceBootstrap(clusterRuntimeConfiguration.getPrimaryInstanceInfo());
            primaryConnectionsStorage.setMaxConnections(clusterRuntimeConfiguration.getMaxPostgresConnections());
        }
        primaryConnectionsStorage.removeAllConnections().forEach(HandlerUtils::closeOnFlush);
        switchoverInProgress.set(false);
        poolActive.set(switchoverCompletedEvent.isSuccess());
    }

    public void listenToStandbyAddedEvent(@Observes(notifyObserver = Reception.IF_EXISTS) StandbyAddedEvent standbyAddedEvent) {
        RuntimePostgresInstanceInfo info = clusterRuntimeConfiguration.getAllPostgresInstancesInfos().get(standbyAddedEvent.getInstanceId());
        if (info == null) {
            return;
        }

        standbyConnectionsStorages.put(
                standbyAddedEvent.getInstanceId(),
                StandbyPostgresPoolWrapper
                        .builder()
                        .standbyBootstrap(createInstanceBootstrap(info))
                        .storage(new PostgresInstancePooledConnectionsStorage(clusterRuntimeConfiguration.getMaxPostgresConnections()))
                        .build()
        );
    }

    public void listenToStandbyRemovedEvent(@Observes(notifyObserver = Reception.IF_EXISTS) StandbyRemovedEvent standbyRemovedEvent) {
        StandbyPostgresPoolWrapper wrapper = standbyConnectionsStorages.remove(standbyRemovedEvent.getInstanceId());
        if (wrapper != null) {
            wrapper.getStorage().removeAllConnections().forEach(HandlerUtils::closeOnFlush);
        }
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
                            AtomicBoolean finished = new AtomicBoolean(false);

                            ScheduledFuture<?> cancelFuture = pooledConnectionInternalInfo.getRealPostgresConnection().eventLoop().schedule(() -> {
                                        if (finished.compareAndSet(false, true)) {
                                            log.warn("Timeout reached for real Postgres connection auth.");
                                            HandlerUtils.closeOnFlush(
                                                    pooledConnectionInternalInfo.getRealPostgresConnection(),
                                                    ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage()
                                            );
                                        } else {
                                            log.debug("Postgres connection acquired. Canceling scheduled...");
                                        }
                                    },
                                    proxyProperties.connectionPool().cleanRealUsedConnectionTimeout().toMillis(),
                                    TimeUnit.MILLISECONDS
                            );

                            pooledConnectionInternalInfo.getRealPostgresConnection().pipeline().addLast(
                                    connectionPoolChannelHandlerProducer.createChannelCleaningHandler(
                                            result -> {
                                                if (finished.compareAndSet(false, true)) {
                                                    cancelFuture.cancel(false);
                                                } else {
                                                    return;
                                                }

                                                if (result.isSuccess()) {
                                                    HandlerUtils.removeAllHandlersFromChannelPipeline(pooledConnectionInternalInfo.getRealPostgresConnection());
                                                    returnTakenConnectionToPoolAndCloseIfFailed(storage, pooledConnectionInternalInfo);
                                                } else {
                                                    HandlerUtils.closeOnFlush(
                                                            pooledConnectionInternalInfo.getRealPostgresConnection(),
                                                            ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage()
                                                    );
                                                }
                                            }
                                    )
                            );

                            pooledConnectionInternalInfo.getRealPostgresConnection().read();
                        } else {
                            returnTakenConnectionToPoolAndCloseIfFailed(storage, pooledConnectionInternalInfo);
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

    private void returnTakenConnectionToPoolAndCloseIfFailed(PostgresInstancePooledConnectionsStorage storage, PooledConnectionInternalInfo pooledConnectionInternalInfo) {
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

    private StandbyPostgresPoolWrapper getLeastLoadedStandbyStorage() {
        return standbyConnectionsStorages.values()
                .stream()
                .min(
                        Comparator.<StandbyPostgresPoolWrapper>comparingInt(s -> s.storage.getFreeConnectionsCount())
                                .reversed()
                                .thenComparing(s -> s.storage.getFreeConnectionsCount())
                )
                .orElse(null);
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    private static class StandbyPostgresPoolWrapper {
        private Bootstrap standbyBootstrap;
        private PostgresInstancePooledConnectionsStorage storage;
    }
}
