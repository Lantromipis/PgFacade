package com.lantromipis.configuration.producers;

import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.configuration.utils.EmptyNettyHandler;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.exception.PgConnectionInitializationException;
import com.lantromipis.postgresprotocol.model.internal.auth.ScramPgAuthInfo;
import com.lantromipis.postgresprotocol.producer.PgFrontendChannelHandlerProducer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@ApplicationScoped
public class RuntimePostgresConnectionProducer {

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    PostgresProperties postgresProperties;

    @Inject
    @Named("worker")
    EventLoopGroup workerGroup;

    @Inject
    @Named("boss")
    EventLoopGroup bossGroup;

    @Inject
    PgFrontendChannelHandlerProducer pgFrontendChannelHandlerProducer;

    public Connection createNewPgFacadeUserConnectionToCurrentPrimary() throws SQLException {
        return createNewPgFacadeUserConnectionToInstance(clusterRuntimeProperties.getPrimaryInstanceId());
    }

    public Connection createNewPgFacadeUserConnectionToInstance(UUID instanceId) throws SQLException {
        if (instanceId == null) {
            return null;
        }

        RuntimePostgresInstanceInfo runtimePostgresInstanceInfo = clusterRuntimeProperties.getAllPostgresInstancesInfos().get(instanceId);
        if (runtimePostgresInstanceInfo == null) {
            return null;
        }

        return createNewPgFacadeUserConnection(
                runtimePostgresInstanceInfo.getAddress(),
                runtimePostgresInstanceInfo.getPort()
        );
    }

    public Connection createNewPgFacadeUserConnection(String address, int port) throws SQLException {
        String jdbcUrl = "jdbc:postgresql://"
                + address
                + ":"
                + port
                + "/"
                + postgresProperties.users().pgFacade().database();

        return DriverManager.getConnection(
                jdbcUrl,
                postgresProperties.users().pgFacade().username(),
                postgresProperties.users().pgFacade().password()
        );
    }

    public Channel createNewNettyChannelToPrimaryForReplication() throws PgConnectionInitializationException {
        return createNewNettyChannelToInstanceForReplication(clusterRuntimeProperties.getPrimaryInstanceId());
    }

    public Channel createNewNettyChannelToInstanceForReplication(UUID instanceId) throws PgConnectionInitializationException {
        Map<String, String> parameters = new HashMap<>();

        parameters.put(PostgresProtocolGeneralConstants.STARTUP_PARAMETER_DATABASE, "replication");
        parameters.put("application_name", "pg_facade");
        parameters.put(PostgresProtocolGeneralConstants.STARTUP_PARAMETER_REPLICATION, "true");
        parameters.put("client_encoding", "UTF8");

        return createNewNettyChannelToInstance(
                instanceId,
                postgresProperties.users().pgFacade().username(),
                postgresProperties.users().pgFacade().password(),
                parameters
        );
    }

    private Channel createNewNettyChannelToInstance(UUID instanceId,
                                                    String user,
                                                    String password,
                                                    Map<String, String> parameters) throws PgConnectionInitializationException {
        if (instanceId == null) {
            return null;
        }

        try {
            parameters.put(PostgresProtocolGeneralConstants.STARTUP_PARAMETER_USER, user);

            Bootstrap primaryBootstrap = createInstanceBootstrap(clusterRuntimeProperties.getInstanceInfo(instanceId));
            ChannelFuture channelFuture = primaryBootstrap.connect();

            ScramPgAuthInfo pgAuthInfo = ScramPgAuthInfo
                    .builder()
                    .passwordKnown(true)
                    .password(password)
                    .build();

            AtomicReference<Channel> ret = new AtomicReference<>(null);

            CountDownLatch countDownLatch = new CountDownLatch(1);
            channelFuture.addListener((ChannelFutureListener) future -> {
                        if (!future.isSuccess()) {
                            countDownLatch.countDown();
                        } else {
                            Channel channel = future.channel();
                            channel.pipeline().remove(EmptyNettyHandler.class);
                            channel.pipeline().addLast(
                                    new LoggingHandler(this.getClass(), LogLevel.DEBUG),
                                    pgFrontendChannelHandlerProducer.createNewChannelStartupHandler(
                                            pgAuthInfo,
                                            parameters,
                                            result -> {
                                                if (result.isSuccess()) {
                                                    ret.set(channel);
                                                    countDownLatch.countDown();
                                                }
                                                countDownLatch.countDown();
                                            }
                                    )
                            );
                        }

                    }
            );

            try {
                boolean success = countDownLatch.await(5000, TimeUnit.MILLISECONDS);
                if (!success) {
                    throw new PgConnectionInitializationException("Timeout reached while trying to acquire connection for Postgres using Netty!");
                }

                Channel channel = ret.get();
                if (channel == null) {
                    throw new PgConnectionInitializationException("Failed to acquire connection for Postgres using Netty!");
                }

                return channel;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
        } catch (PgConnectionInitializationException e) {
            throw e;
        } catch (Exception e) {
            throw new PgConnectionInitializationException("Failed to acquire connection for Postgres using Netty!", e);
        }
    }

    private Bootstrap createInstanceBootstrap(RuntimePostgresInstanceInfo instanceInfo) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class)
                .option(ChannelOption.AUTO_READ, false)
                .handler(EmptyNettyHandler.SHRED_EMPTY_HANDLER)
                .remoteAddress(
                        instanceInfo.getAddress(),
                        instanceInfo.getPort()
                );

        return bootstrap;
    }
}
