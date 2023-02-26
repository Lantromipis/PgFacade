package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.PostgresConfigurationChangeException;

import java.util.Map;
import java.util.UUID;

/**
 * Help service for Orchestrator, so it should only be used by orchestrator
 */
public interface PostgresConfigurator {

    /**
     * Method used to initialize this service
     */
    void initialize();

    /**
     * Method used to configure new master using superuser credentials from application.yaml
     * This method must work BEFORE initialize() is called.
     */
    void configureNewlyCreatedMaster();

    /**
     * Method is used to change Postgres settings. It is an automated alternative to manually modifying postgresql.conf or calling ALTER SYSTEM.
     *
     * @param newSettingNamesAndValuesMap new settings that will be applied
     * @return true if restart is needed, and false if not
     * @throws PostgresConfigurationChangeException when something went wrong during settings update
     */
    boolean changePostgresSettings(UUID instanceId, Map<String, String> newSettingNamesAndValuesMap) throws PostgresConfigurationChangeException;
}
