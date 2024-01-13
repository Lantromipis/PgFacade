package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.model.PostgresInstanceSettingsChangeResult;
import com.lantromipis.orchestration.model.PostgresSettingsValidationResult;

import java.sql.Connection;
import java.util.Map;
import java.util.UUID;

/**
 * Help service for Orchestrator, so it should only be used by orchestrator
 */
public interface PostgresConfigurationService {

    /**
     * Method is used to validate settings and check if new settings require restart.
     *
     * @param settingsToCheck map of settings that will be checked. Key is setting name, value is setting value
     * @return object containing validation result
     */
    PostgresSettingsValidationResult validateSettingAndCheckIfRestartRequired(Map<String, String> settingsToCheck);

    /**
     * Changes settings of Postgres instance
     *
     * @param instanceId                  persisted Postgres instance id
     * @param newSettingNamesAndValuesMap settings which will be applied
     * @return object containing result of settings change
     */
    PostgresInstanceSettingsChangeResult changePostgresInstanceSettingsAndRollbackOnFailure(UUID instanceId, Map<String, String> newSettingNamesAndValuesMap);

    /**
     * Method is used to fast change any Postgres setting. However, there are no checks that settings are valid nor rollback in case of failure.
     * Must be used carefully when it is critical to apply settings fast (ex. switchover)
     *
     * @param newSettingNamesAndValuesMap new settings that will be applied. Key is setting name, value is setting value.
     *                                    Settings will be merged with existing one in special PgFacade conf file.
     * @param reloadConf                  if true, pg_reload_conf() will be called
     * @param connection                  JDBC connection which will be used.
     * @return true if success, false if not
     */
    boolean changePostgresSettingsFastUnsafe(Map<String, String> newSettingNamesAndValuesMap, boolean reloadConf, Connection connection);
}
