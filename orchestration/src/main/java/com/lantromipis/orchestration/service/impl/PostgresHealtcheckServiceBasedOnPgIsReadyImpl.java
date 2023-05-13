package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.orchestration.constant.CommandsConstants;
import com.lantromipis.orchestration.service.api.PostgresHealthcheckService;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@Slf4j
@ApplicationScoped
public class PostgresHealtcheckServiceBasedOnPgIsReadyImpl implements PostgresHealthcheckService {

    @Inject
    PostgresProperties postgresProperties;

    @Override
    public boolean isHealthyWithoutAuth(String address, int port, long timeout) {
        String shellCommand = CommandsConstants.PG_IS_READY_COMMAND + " "
                + CommandsConstants.PG_IS_READY_COMMAND_HOST_KEY + " " + address
                + " "
                + CommandsConstants.PG_IS_READY_COMMAND_PORT_KEY + " " + port
                + " "
                + CommandsConstants.PG_IS_READY_COMMAND_TIMEOUT_KEY + " " + timeout
                + " "
                // not to spam errors in Postgres log that user with role root does not exist.
                + CommandsConstants.PG_IS_READY_COMMAND_USER_KEY + " " + postgresProperties.users().pgFacade().username()
                + " "
                + CommandsConstants.PG_IS_READY_COMMAND_QUITE_KEY;

        try {
            ProcessBuilder pgIsReadyProcessBuilder = new ProcessBuilder();
            pgIsReadyProcessBuilder.command("/bin/sh", "-c", shellCommand);
            Process process = pgIsReadyProcessBuilder.start();
            int code = process.waitFor();

            return code == 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Failed to execute pg_isready command", e);
            return false;
        } catch (Exception e) {
            log.error("Failed to execute pg_isready command", e);
            return false;
        }
    }
}
