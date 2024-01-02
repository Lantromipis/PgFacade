package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.producers.RuntimePostgresConnectionProducer;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.orchestration.service.api.PostgresHealthcheckService;
import com.lantromipis.postgresprotocol.handler.frontend.PgChannelSimpleQueryExecutorHandler;
import com.lantromipis.postgresprotocol.utils.PostgresErrorMessageUtils;
import com.lantromipis.postgresprotocol.utils.PostgresHandlerUtils;
import io.netty.channel.Channel;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ApplicationScoped
public class PostgresHealtcheckServiceImpl implements PostgresHealthcheckService {

    @Inject
    PostgresProperties postgresProperties;

    @Inject
    RuntimePostgresConnectionProducer runtimePostgresConnectionProducer;

    private static final String SIMPLE_HEALTHCHECK_QUERY = ";";

    @Override
    public boolean checkPostgresLiveliness(String address, int port, long timeout) {
        Channel pgChannel = null;
        try {
            pgChannel = runtimePostgresConnectionProducer.createNewNettyChannelToInstanceUsingPgFacadeUser(address, port, timeout);
            if (pgChannel == null) {
                log.error("Failed to execute Postgres liveliness check because connection attempt failed!");
                return false;
            }

            PgChannelSimpleQueryExecutorHandler queryExecutor = new PgChannelSimpleQueryExecutorHandler();
            boolean handlerAdded = PostgresHandlerUtils.addHandlerLastAndAwaitActive(pgChannel.pipeline(), queryExecutor, 100);

            if (!handlerAdded) {
                log.error("Failed to execute " + SIMPLE_HEALTHCHECK_QUERY + " SQL query for Postgres liveliness check due to internal timeout!");
                return false;
            }

            PgChannelSimpleQueryExecutorHandler.CommandExecutionResult executionResult = queryExecutor.executeQueryBlocking(SIMPLE_HEALTHCHECK_QUERY, timeout);
            switch (executionResult.getStatus()) {
                case SUCCESS -> {
                    return true;
                }
                case SERVER_ERROR -> {
                    log.error("Failed to execute " + SIMPLE_HEALTHCHECK_QUERY + " SQL query for Postgres liveliness check due to server error! Message from server: " + PostgresErrorMessageUtils.getLoggableErrorMessageFromErrorResponse(executionResult.getErrorResponse()));
                    return false;
                }
                case TIMEOUT -> {
                    log.error("Failed to execute " + SIMPLE_HEALTHCHECK_QUERY + " SQL query for Postgres liveliness check due to response timeout!");
                    return false;
                }
                default -> {
                    log.error("Failed to execute " + SIMPLE_HEALTHCHECK_QUERY + " SQL query for Postgres liveliness check due to client error!");
                    return false;
                }
            }
        } catch (Throwable t) {
            log.error("Exception while trying to check Postgres instance health. ", t);
            return false;
        } finally {
            PostgresHandlerUtils.closeGracefullyOnFlush(pgChannel);
        }
    }
}
