package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.PostgresConfigurationChangeException;
import com.lantromipis.orchestration.exception.PostgresConfigurationCheckException;

import java.util.Map;
import java.util.UUID;

/**
 * Help service for Orchestrator, so it should only be used by orchestrator
 */
public interface PostgresConfigurator {

    /**
     * Method used to initialize configurator service. This method must load some important runtime properties based on current primary configuration (like max_connections)
     */
    void initialize();

    /**
     * Method used to configure new master using superuser credentials from application.yaml
     * This method must work BEFORE initialize() is called.
     */
    void configureNewlyCreatedMaster();

    /**
     * Method is used to validate settings and check if new settings require restart.
     *
     * @param settingsToCheck map of settings that will be checked. Key is setting name, value is setting value
     * @return true if restart is needed, and false if not
     * @throws PostgresConfigurationChangeException when some settings are invalid
     */
    boolean validateSettingAndCheckIfRestartRequired(Map<String, String> settingsToCheck) throws PostgresConfigurationCheckException;

    /**
     * Method is used to change Postgres settings. It is an automated alternative to manually modifying postgresql.conf or calling ALTER SYSTEM.
     *
     * @param newSettingNamesAndValuesMap new settings that will be applied. Key is setting name, value is setting value
     * @return true if restart is needed, and false if not
     * @throws PostgresConfigurationCheckException  when some settings are invalid
     * @throws PostgresConfigurationChangeException when something went wrong during settings update
     */
    boolean changePostgresSettings(UUID instanceId, Map<String, String> newSettingNamesAndValuesMap) throws PostgresConfigurationCheckException, PostgresConfigurationChangeException;
}
