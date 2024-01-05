package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.model.PgSetting;
import com.lantromipis.configuration.producers.RuntimePostgresConnectionProducer;
import com.lantromipis.configuration.properties.constant.PostgresConstants;
import com.lantromipis.configuration.properties.runtime.PostgresSettingsRuntimeProperties;
import com.lantromipis.orchestration.model.PostgresInstanceSettingsChangeResult;
import com.lantromipis.orchestration.model.PostgresSettingsValidationResult;
import com.lantromipis.orchestration.service.api.PostgresConfigurator;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

@Slf4j
@ApplicationScoped
public class PostgresConfiguratorImpl implements PostgresConfigurator {

    @Inject
    RuntimePostgresConnectionProducer runtimePostgresConnectionProducer;

    @Inject
    PostgresSettingsRuntimeProperties postgresSettingsRuntimeProperties;

    @Override
    public PostgresSettingsValidationResult validateSettingAndCheckIfRestartRequired(Map<String, String> settingsToCheck) {
        boolean restartRequired = false;
        Map<String, String> settingNameToError = new HashMap<>();

        //TODO add check for types using vartype and enumvals columns
        Map<String, PgSetting> settingsInfo = postgresSettingsRuntimeProperties.getCachedSettings();
        for (var entry : settingsToCheck.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }

            String settingName = entry.getKey();
            String settingContext = Optional.ofNullable(settingsInfo.get(settingName))
                    .map(PgSetting::getContext)
                    .orElse(null);

            if (settingContext == null) {
                settingNameToError.put(settingName, "Unknown setting provided. Can not find it in pg_settings table.");
            } else {
                if (PostgresConstants.FORBIDDEN_TO_CHANGE_SETTINGS_NAMES.contains(settingName)) {
                    settingNameToError.put(settingName, "Unable to change setting because it is managed by PgFacade.");
                }
                if (PostgresConstants.UNMODIFIABLE_SETTINGS_CONTEXT_NAMES.contains(settingContext)) {
                    settingNameToError.put(settingName, "Unable to change setting. This setting is immutable.");
                }
                if (PostgresConstants.RESTART_REQUIRED_SETTINGS_CONTEXT_NAMES.contains(settingContext)) {
                    restartRequired = true;
                }
            }
        }

        return PostgresSettingsValidationResult
                .builder()
                .settingsValid(settingNameToError.isEmpty())
                .restartRequired(restartRequired)
                .settingNameToError(settingNameToError)
                .build();
    }

    @Override
    public PostgresInstanceSettingsChangeResult changePostgresInstanceSettingsAndRollbackOnFailure(UUID instanceId, Map<String, String> newSettingNamesAndValuesMap) {
        PostgresSettingsValidationResult validationResult = validateSettingAndCheckIfRestartRequired(newSettingNamesAndValuesMap);

        if (!validationResult.isSettingsValid()) {
            return PostgresInstanceSettingsChangeResult
                    .builder()
                    .status(PostgresInstanceSettingsChangeResult.Status.SETTINGS_INVALID)
                    .settingNameToValidationError(validationResult.getSettingNameToError())
                    .build();
        }

        Connection connection = null;
        Statement oldPgSettingsStatement = null;
        ResultSet oldPgSettingsResultSet;
        try {
            connection = runtimePostgresConnectionProducer.createNewPgFacadeUserConnectionToInstance(instanceId);

            oldPgSettingsStatement = connection.createStatement();
            oldPgSettingsResultSet = oldPgSettingsStatement.executeQuery("SELECT * FROM pg_settings");
        } catch (Exception e) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception ignored) {
                }
            }
            if (e instanceof SQLException) {
                log.error("Failed to change postgres settings due to SQL error!", e);
                return PostgresInstanceSettingsChangeResult
                        .builder()
                        .status(PostgresInstanceSettingsChangeResult.Status.SQL_ERROR)
                        .sqlErrorMessage(e.getMessage())
                        .build();
            } else {
                log.error("Failed to change postgres settings due to internal error!", e);
                return PostgresInstanceSettingsChangeResult
                        .builder()
                        .status(PostgresInstanceSettingsChangeResult.Status.INTERNAL_ERROR)
                        .build();
            }
        }

        Set<String> appliedSettingsNames = new HashSet<>();
        try {
            for (var entry : newSettingNamesAndValuesMap.entrySet()) {
                executeAlterSystem(entry.getKey(), entry.getValue(), connection);
                appliedSettingsNames.add(entry.getKey());
            }

            reloadConf(connection);

            return PostgresInstanceSettingsChangeResult
                    .builder()
                    .status(PostgresInstanceSettingsChangeResult.Status.SUCCESS)
                    .restartRequired(validationResult.isRestartRequired())
                    .build();
        } catch (Exception e) {
            if (!appliedSettingsNames.isEmpty()) {
                log.error("Failed to change Postgres settings! Will try to rollback made changes!", e);
                Set<String> successfullyRollbackedSettings = rollbackSettingsSafe(oldPgSettingsResultSet, appliedSettingsNames, connection);
                appliedSettingsNames.removeAll(successfullyRollbackedSettings);
                if (!appliedSettingsNames.isEmpty()) {
                    log.error("Was not able to rollback settings {}", appliedSettingsNames);
                } else {
                    log.info("All other setting in current change request was successfully rollbacked!");
                }
            } else {
                log.error("Failed to change Postgres settings!", e);
            }

            if (e instanceof SQLException) {
                return PostgresInstanceSettingsChangeResult
                        .builder()
                        .status(PostgresInstanceSettingsChangeResult.Status.SQL_ERROR)
                        .rollbackWasRequired(true)
                        .notRollbackedSettings(appliedSettingsNames)
                        .sqlErrorMessage(e.getMessage())
                        .build();
            } else {
                return PostgresInstanceSettingsChangeResult
                        .builder()
                        .status(PostgresInstanceSettingsChangeResult.Status.INTERNAL_ERROR)
                        .rollbackWasRequired(true)
                        .notRollbackedSettings(appliedSettingsNames)
                        .build();
            }
        } finally {
            try {
                connection.close();
            } catch (Exception ignored) {
            }
            try {
                connection.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Override
    public boolean changePostgresSettingsFastUnsafe(Map<String, String> newSettingNamesAndValuesMap, boolean reloadConf, Connection connection) {
        try {
            for (var entry : newSettingNamesAndValuesMap.entrySet()) {
                executeAlterSystem(entry.getKey(), entry.getValue(), connection);
            }

            if (reloadConf) {
                reloadConf(connection);
            }

            return true;
        } catch (Exception e) {
            log.error("Error while applying new Postgres settings using fast and unsafe method!", e);
            return false;
        }
    }

    private void executeAlterSystem(String settingName, String settingValue, Connection connection) throws SQLException {
        @Cleanup Statement statement = connection.createStatement();
        statement.execute(
                String.format(
                        "ALTER SYSTEM SET %s = '%s'",
                        settingName,
                        settingValue
                )
        );
    }

    /**
     * Rollback settings and report all successfully rollbacked ones. Fails on first error.
     *
     * @param oldPgSettingsResultSet result set containing pg_settings table content at the moment to when rollback is required
     * @param rollbackSettingsNames  settings names to rollback
     * @param connection             JDBC connection
     * @return Set containing settings which were successfully rollbacked
     */
    private Set<String> rollbackSettingsSafe(ResultSet oldPgSettingsResultSet, Set<String> rollbackSettingsNames, Connection connection) {
        Set<String> rollbackedSettings = new HashSet<>();
        try {
            while (oldPgSettingsResultSet.next()) {
                String settingName = oldPgSettingsResultSet.getString("name");
                String settingValue = oldPgSettingsResultSet.getString("setting");

                if (rollbackSettingsNames.contains(settingName)) {
                    executeAlterSystem(settingName, settingValue, connection);
                    rollbackedSettings.add(settingName);
                }
            }
        } catch (Exception e) {
            log.error("Failed to rollback settings!", e);
        }

        return rollbackedSettings;
    }


    private void reloadConf(Connection connection) throws SQLException {
        connection.createStatement().execute("SELECT pg_reload_conf()");
    }
}
