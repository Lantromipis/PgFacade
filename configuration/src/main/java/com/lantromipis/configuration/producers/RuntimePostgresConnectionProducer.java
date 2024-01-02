package com.lantromipis.configuration.producers;

import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.configuration.utils.EmptyNettyHandler;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.model.internal.auth.ScramPgAuthInfo;
import com.lantromipis.postgresprotocol.producer.PgFrontendChannelHandlerProducer;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import com.lantromipis.postgresprotocol.utils.PostgresErrorMessageUtils;
import com.lantromipis.postgresprotocol.utils.PostgresHandlerUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private static final long DEFAULT_CONNECTION_CREATION_TIMEOUT_MS = 5000;

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

        return createNewPgFacadeUserConnection(runtimePostgresInstanceInfo.getAddress(), runtimePostgresInstanceInfo.getPort());
    }

    public Connection createNewPgFacadeUserConnection(String address, int port) throws SQLException {
        String jdbcUrl = "jdbc:postgresql://" + address + ":" + port + "/" + postgresProperties.users().pgFacade().database();

        return DriverManager.getConnection(jdbcUrl, postgresProperties.users().pgFacade().username(), postgresProperties.users().pgFacade().password());
    }

    public Channel createNewNettyChannelToPrimaryForReplication() {
        return createNewNettyChannelToInstanceForReplication(clusterRuntimeProperties.getPrimaryInstanceId());
    }

    public Channel createNewNettyChannelToInstanceForReplication(UUID instanceId) {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(PostgresProtocolGeneralConstants.STARTUP_PARAMETER_REPLICATION, "true");

        return createNewNettyChannelToPostgres(instanceId, "replication", postgresProperties.users().replication().username(), postgresProperties.users().replication().password(), parameters, DEFAULT_CONNECTION_CREATION_TIMEOUT_MS);
    }

    public Channel createNewNettyChannelToInstanceUsingPgFacadeUser(UUID instanceId) {
        return createNewNettyChannelToInstanceUsingPgFacadeUser(instanceId, DEFAULT_CONNECTION_CREATION_TIMEOUT_MS);
    }

    public Channel createNewNettyChannelToInstanceUsingPgFacadeUser(UUID instanceId, long timeoutMs) {
        return createNewNettyChannelToPostgres(instanceId, postgresProperties.users().pgFacade().database(), postgresProperties.users().pgFacade().username(), postgresProperties.users().pgFacade().password(), null, timeoutMs);
    }

    public Channel createNewNettyChannelToInstanceUsingPgFacadeUser(String address, int port) {
        return createNewNettyChannelToInstanceUsingPgFacadeUser(address, port, DEFAULT_CONNECTION_CREATION_TIMEOUT_MS);
    }

    public Channel createNewNettyChannelToInstanceUsingPgFacadeUser(String address, int port, long timeoutMs) {
        return createNewNettyChannelToPostgres(address, port, postgresProperties.users().pgFacade().database(), postgresProperties.users().pgFacade().username(), postgresProperties.users().pgFacade().password(), null, timeoutMs);
    }

    private Channel createNewNettyChannelToPostgres(UUID instanceId, String database, String user, String password, Map<String, String> parameters, long timeoutMs) {
        RuntimePostgresInstanceInfo runtimePostgresInstanceInfo = clusterRuntimeProperties.getInstanceInfo(instanceId);
        if (runtimePostgresInstanceInfo == null) {
            log.error("Failed to acquire connection for Postgres for internal needs because instance with UUID {} is unknown!", instanceId);
            return null;
        }

        return createNewNettyChannelToPostgres(runtimePostgresInstanceInfo.getAddress(), runtimePostgresInstanceInfo.getPort(), database, user, password, parameters, timeoutMs);
    }

    private Channel createNewNettyChannelToPostgres(String address, int port, String database, String user, String password, Map<String, String> parameters, long timeoutMs) {
        try {
            Map<String, String> resultParameters = new HashMap<>();
            resultParameters.put("application_name", "pg_facade");
            resultParameters.put("client_encoding", "UTF8");
            resultParameters.put(PostgresProtocolGeneralConstants.STARTUP_PARAMETER_DATABASE, database);
            resultParameters.put(PostgresProtocolGeneralConstants.STARTUP_PARAMETER_USER, user);

            if (MapUtils.isNotEmpty(parameters)) {
                resultParameters.putAll(parameters);
            }

            Bootstrap primaryBootstrap = createInstanceBootstrap(address, port);

            ChannelFuture channelFuture = primaryBootstrap.connect();
            long startTime = System.currentTimeMillis();
            boolean awaitSucceeded = channelFuture.awaitUninterruptibly(timeoutMs, TimeUnit.MILLISECONDS);

            if (!awaitSucceeded) {
                channelFuture.addListener((ChannelFutureListener) future -> {
                    if (future.isSuccess()) {
                        PostgresHandlerUtils.closeGracefullyOnFlush(future.channel());
                    }
                });
                log.error("Failed to acquire connection for Postgres for internal needs due to timeout!");
                return null;
            }

            long leftTimeout = timeoutMs - (System.currentTimeMillis() - startTime);
            Channel pgChannel = channelFuture.channel();

            ScramPgAuthInfo pgAuthInfo = ScramPgAuthInfo.builder().passwordKnown(true).password(password).build();

            AtomicBoolean authSucceeded = new AtomicBoolean(false);
            CountDownLatch countDownLatch = new CountDownLatch(1);

            pgChannel.pipeline().remove(EmptyNettyHandler.class);
            pgChannel.pipeline().addLast(pgFrontendChannelHandlerProducer.createNewChannelStartupHandler(
                    pgAuthInfo,
                    resultParameters,
                    result -> {
                        authSucceeded.set(result.isSuccess());
                        if (!result.isSuccess()) {
                            if (result.getErrorResponse() != null) {
                                log.error("Failed to acquire connection for Postgres for internal needs because auth failed! Received error from server: {}", PostgresErrorMessageUtils.getLoggableErrorMessageFromErrorResponse(result.getErrorResponse()));
                            } else {
                                log.error("Failed to acquire connection for Postgres for internal needs because auth failed!");
                            }
                        }

                        DecoderUtils.freeMessageInfos(result.getServerStartMessagesInfos());
                        countDownLatch.countDown();
                    }));

            try {
                boolean noTimeout = countDownLatch.await(leftTimeout, TimeUnit.MILLISECONDS);
                if (!noTimeout) {
                    log.error("Failed to acquire connection for Postgres for internal needs due to timeout!");
                    PostgresHandlerUtils.closeGracefullyOnFlush(pgChannel);
                    return null;
                }

                if (!authSucceeded.get()) {
                    PostgresHandlerUtils.closeGracefullyOnFlush(pgChannel);
                    return null;
                }

                return pgChannel;
            } catch (InterruptedException e) {
                PostgresHandlerUtils.closeGracefullyOnFlush(pgChannel);
                log.error("Failed to acquire connection for Postgres for internal needs because thread was interrupted!");
                Thread.currentThread().interrupt();
                return null;
            }
        } catch (Exception e) {
            log.error("Failed to acquire connection for Postgres for internal needs due to exception!", e);
            return null;
        }
    }

    private Bootstrap createInstanceBootstrap(String address, int port) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup).channel(Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class).option(ChannelOption.AUTO_READ, false).handler(EmptyNettyHandler.SHRED_EMPTY_HANDLER).remoteAddress(address, port);

        return bootstrap;
    }
}
