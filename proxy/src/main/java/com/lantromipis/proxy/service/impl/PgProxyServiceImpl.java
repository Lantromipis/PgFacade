package com.lantromipis.proxy.service.impl;

import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import com.lantromipis.configuration.properties.predefined.ProxyProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.configuration.properties.runtime.PostgresSettingsRuntimeProperties;
import com.lantromipis.connectionpool.handler.EmptyHandler;
import com.lantromipis.postgresprotocol.handler.frontend.PgChannelSimpleQueryExecutorHandler;
import com.lantromipis.postgresprotocol.handler.frontend.PgStreamingReplicationHandler;
import com.lantromipis.postgresprotocol.model.PgResultSet;
import com.lantromipis.postgresprotocol.model.internal.PgLogSequenceNumber;
import com.lantromipis.postgresprotocol.model.internal.auth.ScramPgAuthInfo;
import com.lantromipis.postgresprotocol.producer.PgFrontendChannelHandlerProducer;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import com.lantromipis.proxy.initializer.PooledProxyChannelInitializer;
import com.lantromipis.proxy.initializer.UnpooledProxyChannelInitializer;
import com.lantromipis.proxy.producer.ProxyChannelHandlersProducer;
import com.lantromipis.proxy.service.api.ClientConnectionsManagementService;
import com.lantromipis.proxy.service.api.PgProxyService;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import lombok.extern.slf4j.Slf4j;

import java.io.RandomAccessFile;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@ApplicationScoped
public class PgProxyServiceImpl implements PgProxyService {
    @Inject
    ProxyProperties proxyProperties;

    @Inject
    ProxyChannelHandlersProducer proxyChannelHandlersProducer;

    @Inject
    ClientConnectionsManagementService clientConnectionsManagementService;

    @Inject
    @Named("worker")
    EventLoopGroup workerGroup;

    @Inject
    @Named("boss")
    EventLoopGroup bossGroup;

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    PgFrontendChannelHandlerProducer pgFrontendChannelHandlerProducer;

    @Inject
    PostgresSettingsRuntimeProperties postgresSettingsRuntimeProperties;

    private ChannelFuture primaryProxyChannelFuture, standbyProxyChannelFuture;

    public void initialize() {
        ServerBootstrap primaryProxyBootstrap = new ServerBootstrap();
        ServerBootstrap standbyProxyBootstrap = new ServerBootstrap();

        Thread primaryBootstrapThread = new Thread(
                () -> {
                    try {
                        ChannelHandler channelInitializer;

                        if (proxyProperties.connectionPool().enabled()) {
                            log.info("Starting primary proxy with connection pool.");
                            channelInitializer = new PooledProxyChannelInitializer(proxyChannelHandlersProducer, clientConnectionsManagementService, true);
                        } else {
                            log.info("Starting primary proxy without connection pool.");
                            channelInitializer = new UnpooledProxyChannelInitializer(true, proxyChannelHandlersProducer, clientConnectionsManagementService);
                        }

                        primaryProxyChannelFuture = primaryProxyBootstrap.group(bossGroup, workerGroup)
                                .channel(Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                                .childHandler(channelInitializer)
                                .childOption(ChannelOption.AUTO_READ, false)
                                .bind(proxyProperties.primaryPort())
                                .sync();

                        log.info("Postgres primary proxy listening on port " + proxyProperties.primaryPort());

                        primaryProxyChannelFuture.channel().closeFuture().sync();
                        log.info("Postgres primary proxy bootstrap stopped. Proxy not accepting connections.");

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        log.error("Error while waiting for primary proxy to close.", e);
                    }
                }
        );
        Thread standbyBootstrapThread = new Thread(
                () -> {
                    try {
                        ChannelHandler channelInitializer;

                        if (proxyProperties.connectionPool().enabled()) {
                            log.info("Starting standby proxy with connection pool.");
                            channelInitializer = new PooledProxyChannelInitializer(proxyChannelHandlersProducer, clientConnectionsManagementService, false);
                        } else {
                            log.info("Starting standby proxy without connection pool.");
                            channelInitializer = new UnpooledProxyChannelInitializer(true, proxyChannelHandlersProducer, clientConnectionsManagementService);
                        }

                        standbyProxyChannelFuture = standbyProxyBootstrap.group(bossGroup, workerGroup)
                                .channel(Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                                .childHandler(channelInitializer)
                                .childOption(ChannelOption.AUTO_READ, false)
                                .bind(proxyProperties.standbyPort())
                                .sync();

                        log.info("Postgres standby proxy listening on port " + proxyProperties.standbyPort());

                        standbyProxyChannelFuture.channel().closeFuture().sync();
                        log.info("Postgres standby proxy bootstrap stopped. Proxy not accepting connections.");

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        log.error("Error while waiting for standby proxy to close.", e);
                    }
                }
        );
        primaryBootstrapThread.start();
        standbyBootstrapThread.start();

        Bootstrap primaryBootstrap = createInstanceBootstrap(RuntimePostgresInstanceInfo
                .builder()
                .primary(true)
                .address("localhost")
                .port(5442)
                .build()
        );
        ChannelFuture channelFuture = primaryBootstrap.connect();

        ScramPgAuthInfo pgAuthInfo = ScramPgAuthInfo
                .builder()
                .passwordKnown(true)
                .password("postgres")
                .build();

        Map<String, String> parameters = new HashMap<>();
        parameters.put("database", "replication");
        parameters.put("user", "postgres");
        parameters.put("application_name", "pg_receivewal");
        parameters.put("replication", "true");
        parameters.put("client_encoding", "UTF8");


        channelFuture.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                Channel channel = future.channel();
                channel.pipeline().remove(EmptyHandler.class);
                channel.pipeline().addLast(
                        new LoggingHandler(this.getClass(), LogLevel.DEBUG),
                        pgFrontendChannelHandlerProducer.createNewChannelStartupHandler(
                                pgAuthInfo,
                                parameters,
                                result -> {
                                    if (result.isSuccess()) {
                                        log.info("Acquired connection for replication");
                                    } else {
                                        log.info("Failed to acquired connection for replication");
                                    }

                                    AtomicReference<PgLogSequenceNumber> processedLsn = new AtomicReference<>(null);
                                    AtomicReference<RandomAccessFile> currentFile = new AtomicReference<>(null);

                                    PgChannelSimpleQueryExecutorHandler queryExecutor = new PgChannelSimpleQueryExecutorHandler();
                                    channel.pipeline().addLast(queryExecutor);
                                    PgStreamingReplicationHandler streamingReplicationHandler = new PgStreamingReplicationHandler();
                                    channel.pipeline().addLast(streamingReplicationHandler);

                                    streamingReplicationHandler.startPhysicalReplication(
                                            "slot1",
                                            "0/05000000",
                                            "00000001",
                                            3000,
                                            startResult -> {
                                            },
                                            errorResult -> {

                                            },
                                            walFragmentResult -> {
                                                PgLogSequenceNumber newLsn = walFragmentResult.getFragmentStartLsn();
                                                PgLogSequenceNumber currentLsn = processedLsn.get();

                                                try {
                                                    RandomAccessFile file;
                                                    if (newLsn.compareIfBelongsToSameWal(currentLsn)) {
                                                        file = currentFile.get();
                                                    } else {
                                                        if (currentFile.get() != null) {
                                                            currentFile.get().close();
                                                        }
                                                        if (processedLsn.get() != null) {
                                                            streamingReplicationHandler.confirmProcessedLsn(processedLsn.get());
                                                        }
                                                        String filePath = "E:\\Dev\\wal_test" + "\\" + newLsn.toWalName("00000001");
                                                        file = new RandomAccessFile(filePath, "rw");
                                                        file.setLength(16 * 1024 * 1024);
                                                        file.seek(0);
                                                        currentFile.set(file);
                                                        log.info("Switched LSN file to {}!", filePath);
                                                    }
                                                    file.write(walFragmentResult.getFragment(), 0, walFragmentResult.getFragmentLength());
                                                } catch (Exception e) {
                                                    throw new RuntimeException(e);
                                                }

                                                processedLsn.set(walFragmentResult.getFragmentStartLsn());

                                            },
                                            streamingCompletedResult -> {
                                                queryExecutor.executeQuery(
                                                        "TIMELINE_HISTORY 2",
                                                        -1,
                                                        pgMessageInfos -> {
                                                            PgResultSet resultSet = DecoderUtils.extractResultSetFromMessages(pgMessageInfos.getMessageInfos());
                                                            int a = 0;
                                                            int b = a;
                                                        }
                                                );


                                                log.info("Streaming completed! Next timeline is {} and next timeline start lsn is {}", streamingCompletedResult.getNextTimeline(), streamingCompletedResult.getNextTimelineStartLsn());
                                            });
                                }
                        )
                );
            } else {
                log.debug("Failed to acquire real postgres connection for replication.");
            }
        });
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

    @Override
    public void shutdown(boolean awaitClients, Duration awaitClientsDuration) {
        if (primaryProxyChannelFuture == null && standbyProxyChannelFuture == null) {
            return;
        }

        if (primaryProxyChannelFuture != null) {
            primaryProxyChannelFuture.channel().close();
        }
        if (standbyProxyChannelFuture != null) {
            standbyProxyChannelFuture.channel().close();
        }

        if (!awaitClients) {
            clientConnectionsManagementService.forceDisconnectAll();
            return;
        }

        long endTime = System.currentTimeMillis() + awaitClientsDuration.toMillis();

        while (System.currentTimeMillis() < endTime) {
            if (clientConnectionsManagementService.getActiveClientsCount() == 0) {
                return;
            }
        }

        log.warn("Some clients are still connected after timeout. Force disconnecting all of them.");
        clientConnectionsManagementService.forceDisconnectAll();
        primaryProxyChannelFuture = null;
        standbyProxyChannelFuture = null;
    }
}
