package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.PostgresConfigurationChangeException;
import com.lantromipis.orchestration.exception.PostgresConfigurationCheckException;

import java.sql.Connection;
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
     * @param instanceId                  id of Postgres instance to which settings will be applied
     * @param newSettingNamesAndValuesMap new settings that will be applied. Key is setting name, value is setting value
     * @return true if restart is needed, and false if not
     * @throws PostgresConfigurationCheckException  when some settings are invalid
     * @throws PostgresConfigurationChangeException when something went wrong during settings update
     */
    boolean changePostgresSettings(UUID instanceId, Map<String, String> newSettingNamesAndValuesMap) throws PostgresConfigurationCheckException, PostgresConfigurationChangeException;

    /**
     * Method is used to fast change any Postgres setting. However, there are no checks that settings are valid.
     * Must be used carefully when it is critical to apply settings fast (ex. switchover)
     *
     * @param instanceId                  id of Postgres instance to which settings will be applied
     * @param newSettingNamesAndValuesMap new settings that will be applied. Key is setting name, value is setting value
     * @param reloadConf                  if true, pg_reload_conf() will be called
     * @param connection                  optional param. If provided, then connection will be used, if not, then new one will be created.
     * @return true if success, false if not
     */
    boolean fastPostgresSettingsChange(UUID instanceId, Map<String, String> newSettingNamesAndValuesMap, boolean reloadConf, Connection connection);
}
