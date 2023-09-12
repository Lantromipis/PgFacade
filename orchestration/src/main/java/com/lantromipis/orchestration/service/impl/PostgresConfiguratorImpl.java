package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.producers.RuntimePostgresConnectionProducer;
import com.lantromipis.configuration.properties.constant.PostgresqlConfConstants;
import com.lantromipis.configuration.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.constant.PostgresConstants;
import com.lantromipis.orchestration.exception.PostgresConfigurationChangeException;
import com.lantromipis.orchestration.exception.PostgresConfigurationCheckException;
import com.lantromipis.orchestration.exception.PostgresConfigurationReadException;
import com.lantromipis.orchestration.model.AdapterShellCommandExecutionResult;
import com.lantromipis.orchestration.model.PgSettingsTableRow;
import com.lantromipis.orchestration.model.PostgresCombinedInstanceInfo;
import com.lantromipis.orchestration.service.api.PostgresConfigurator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;

@Slf4j
@ApplicationScoped
public class PostgresConfiguratorImpl implements PostgresConfigurator {

    @Inject
    PlatformAdapter platformAdapter;

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    RuntimePostgresConnectionProducer runtimePostgresConnectionProducer;

    @Override
    public void initialize() {
        try {
            Connection connection = runtimePostgresConnectionProducer.createNewPgFacadeUserConnectionToCurrentPrimary();
            ResultSet pgSettingsResultSet = connection.createStatement().executeQuery("SELECT name, setting, context FROM pg_settings");

            // remember connection limit
            while (pgSettingsResultSet.next()) {
                String settingName = pgSettingsResultSet.getString("name");
                if (PostgresConstants.MAX_CONNECTIONS_SETTING_NAME.equals(settingName)) {
                    clusterRuntimeProperties.setMaxPostgresConnections(Integer.parseInt(pgSettingsResultSet.getString("setting")));
                    break;
                }
            }

            clusterRuntimeProperties.setPostgresVersion(extractPostgresVersion(connection));

            connection.close();
        } catch (Exception e) {
            log.error("Error during configurator initialization.", e);
        }
    }

    @Override
    public boolean validateSettingAndCheckIfRestartRequired(Map<String, String> settingsToCheck) throws PostgresConfigurationCheckException {
        try (Connection connection = runtimePostgresConnectionProducer.createNewPgFacadeUserConnectionToCurrentPrimary()) {

            //check for nulls
            for (var entry : settingsToCheck.entrySet()) {
                if (entry.getKey() == null || entry.getValue() == null) {
                    throw new PostgresConfigurationCheckException("Nor key or value of setting can be null.");
                }
            }

            //checking if trying to change forbidden settings
            for (String forbiddenSettingName : PostgresConstants.FORBIDDEN_TO_CHANGE_SETTINGS_NAMES) {
                if (settingsToCheck.containsKey(forbiddenSettingName)) {
                    throw new PostgresConfigurationCheckException("Unable to change setting '" + forbiddenSettingName + "' because it is managed by PgFacade.");
                }
            }

            // checking if max_connections is lower than needed
            if (settingsToCheck.containsKey(PostgresConstants.MAX_CONNECTIONS_SETTING_NAME)) {
                int maxConnectionsSettingValue = Integer.parseInt(settingsToCheck.get(PostgresConstants.MAX_CONNECTIONS_SETTING_NAME));
                if (maxConnectionsSettingValue <= PostgresqlConfConstants.PG_FACADE_RESERVED_CONNECTIONS_COUNT) {
                    throw new PostgresConfigurationCheckException("'max_connections' settings must be greater than " + PostgresqlConfConstants.PG_FACADE_RESERVED_CONNECTIONS_COUNT + " due to builtin configuration value.");
                }
            }

            boolean restartRequired = false;

            //TODO add check for types using vartype and enumvals columns
            Map<String, PgSettingsTableRow> settingNameAndContextMap = getPgSettingsMap(connection);

            for (String settingName : settingsToCheck.keySet()) {
                String settingContext = Optional.ofNullable(settingNameAndContextMap.get(settingName)).map(PgSettingsTableRow::getContext).orElse(null);
                if (settingContext == null) {
                    throw new PostgresConfigurationCheckException("Unknown setting '" + settingName + "' provided. Can not find it in pg_settings table.");
                }
                if (PostgresConstants.UNMODIFIABLE_SETTINGS_CONTEXT_NAMES.contains(settingContext)) {
                    throw new PostgresConfigurationCheckException("Unable to change setting '" + settingName + "'. This setting is immutable.");
                }
                if (PostgresConstants.RESTART_REQUIRED_SETTINGS_CONTEXT_NAMES.contains(settingContext)) {
                    restartRequired = true;
                }
            }

            return restartRequired;

        } catch (PostgresConfigurationCheckException postgresConfigurationCheckException) {
            throw postgresConfigurationCheckException;
        } catch (Exception e) {
            throw new PostgresConfigurationCheckException("Unexpected error occurred while trying to check new Postgres settings. ", e);
        }
    }

    @Override
    public boolean changePostgresSettings(PostgresCombinedInstanceInfo combinedInstanceInfo, Map<String, String> newSettingNamesAndValuesMap) throws PostgresConfigurationCheckException, PostgresConfigurationChangeException {
        if (MapUtils.isEmpty(newSettingNamesAndValuesMap)) {
            return false;
        }

        boolean restartRequired = validateSettingAndCheckIfRestartRequired(newSettingNamesAndValuesMap);

        log.info("Changing settings for instance {}. Restart will be required: {}", combinedInstanceInfo.getPersisted().getInstanceId(), restartRequired);

        try (Connection connection = runtimePostgresConnectionProducer.createNewPgFacadeUserConnectionToInstance(combinedInstanceInfo.getPersisted().getInstanceId())) {
            Map<String, PgSettingsTableRow> settingNameAndContextMap = getPgSettingsMap(connection);

            String filePath = getPgFacadePostgresqlConfFilePath(connection);
            List<String> oldConfLines = getFileLines(combinedInstanceInfo.getAdapter().getAdapterInstanceId(), filePath);

            fastPostgresSettingsChange(combinedInstanceInfo, newSettingNamesAndValuesMap, false, connection);

            ResultSet checkSettingResultSet = connection.createStatement().executeQuery("SELECT * FROM pg_file_settings WHERE error NOTNULL AND sourcefile = '" + filePath + "'");

            List<String> errors = new ArrayList<>();

            while (checkSettingResultSet.next()) {
                String settingsContext = Optional.ofNullable(settingNameAndContextMap.get(checkSettingResultSet.getString("name"))).map(PgSettingsTableRow::getContext).orElse(null);

                if (settingsContext == null) {
                    errors.add("Can not identify error. Check parameters names.");
                }

                // error appears for any settings that require restart
                // don't need to display them as real errors to user
                if (!PostgresConstants.RESTART_REQUIRED_SETTINGS_CONTEXT_NAMES.contains(settingsContext)) {
                    try {
                        errors.add("Parameter name: '" + checkSettingResultSet.getString("name") + "'." + " Parameter value: '" + checkSettingResultSet.getString("setting") + "'." + " Error: '" + checkSettingResultSet.getString("error") + "'");
                    } catch (Exception e) {
                        errors.add("Can not identify error. Check parameters names.");
                    }
                }
            }

            if (CollectionUtils.isNotEmpty(errors)) {
                replaceFileLines(combinedInstanceInfo.getAdapter().getAdapterInstanceId(), filePath, oldConfLines);
                String errorString = String.join("; \n", errors);
                throw new PostgresConfigurationChangeException("Error while applying parameters. " + errorString);
            }

            reloadConf(connection);

            return restartRequired;

        } catch (PostgresConfigurationChangeException postgresConfigurationChangeException) {
            throw postgresConfigurationChangeException;
        } catch (Exception e) {
            throw new PostgresConfigurationChangeException("Unexpected error occurred while trying to apply new Postgres settings. ", e);
        }
    }

    @Override
    public boolean fastPostgresSettingsChange(PostgresCombinedInstanceInfo combinedInstanceInfo, Map<String, String> newSettingNamesAndValuesMap, boolean reloadConf, Connection connection) {
        try {
            boolean closeConnection = false;

            if (connection == null) {
                closeConnection = true;
                connection = runtimePostgresConnectionProducer.createNewPgFacadeUserConnectionToInstance(combinedInstanceInfo.getPersisted().getInstanceId());
            }

            String filePath = getPgFacadePostgresqlConfFilePath(connection);

            List<String> oldConfLines = getFileLines(combinedInstanceInfo.getAdapter().getAdapterInstanceId(), filePath);

            Map<String, String> newConfSettingsMap = new HashMap<>();
            for (String settingLine : oldConfLines) {
                Matcher settingLineMatcher = PostgresConstants.CONF_FILE_LINE_PATTERN.matcher(settingLine);
                if (!settingLineMatcher.matches()) {
                    throw new PostgresConfigurationChangeException("Corrupted config file.");
                }
                newConfSettingsMap.put(settingLineMatcher.group(1), settingLineMatcher.group(2));
            }

            newConfSettingsMap.putAll(newSettingNamesAndValuesMap);

            List<String> newConfLines = new ArrayList<>();
            for (var settingEntry : newConfSettingsMap.entrySet()) {
                newConfLines.add(String.format(PostgresConstants.CONF_FILE_LINE_FORMAT, settingEntry.getKey(), settingEntry.getValue()));
            }

            replaceFileLines(combinedInstanceInfo.getAdapter().getAdapterInstanceId(), filePath, newConfLines);

            if (reloadConf) {
                reloadConf(connection);
            }

            if (closeConnection) {
                connection.close();
            }

            return true;
        } catch (Exception e) {
            log.error("Error while applying settings", e);
            return false;
        }
    }

    private Map<String, PgSettingsTableRow> getPgSettingsMap(Connection connection) throws SQLException {
        ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM pg_settings");

        Map<String, PgSettingsTableRow> ret = new HashMap<>();
        while (resultSet.next()) {
            ret.put(resultSet.getString("name"), PgSettingsTableRow.builder().name(resultSet.getString("name")).value(resultSet.getString("setting")).unit(resultSet.getString("unit")).vartype(resultSet.getString("vartype")).enumvals(resultSet.getString("enumvals")).context(resultSet.getString("context")).build());
        }

        return ret;
    }

    private void reloadConf(Connection connection) throws SQLException {
        connection.createStatement().execute("SELECT pg_reload_conf()");
    }

    private List<String> getFileLines(String adapterIdentifier, String filePath) throws PostgresConfigurationReadException {
        AdapterShellCommandExecutionResult adapterShellCommandExecutionResult = platformAdapter.executeShellCommandForInstance(
                adapterIdentifier,
                "cat " + filePath,
                Collections.emptyList()
        );

        if (!adapterShellCommandExecutionResult.isSuccess() || StringUtils.isNotEmpty(adapterShellCommandExecutionResult.getStderr())) {
            throw new PostgresConfigurationReadException("Unable to read Postgres settings file. Cause: " + adapterShellCommandExecutionResult.getStderr());
        }

        if (StringUtils.isEmpty(adapterShellCommandExecutionResult.getStdout())) {
            return Collections.emptyList();
        }

        return Arrays.asList(adapterShellCommandExecutionResult.getStdout().split("\n"));
    }

    private void replaceFileLines(String adapterIdentifier, String filePath, List<String> newLines) throws PostgresConfigurationChangeException {
        AdapterShellCommandExecutionResult adapterShellCommandExecutionResult = platformAdapter.executeShellCommandForInstance(
                adapterIdentifier,
                "echo \"" + String.join("\n", newLines) + "\" > " + filePath,
                Collections.emptyList()
        );

        if (!adapterShellCommandExecutionResult.isSuccess() || StringUtils.isNotEmpty(adapterShellCommandExecutionResult.getStderr())) {
            throw new PostgresConfigurationChangeException("Unable to change Postgres settings file. Cause: " + adapterShellCommandExecutionResult.getStderr());
        }
    }

    private String getPgHbaConfFilePath(Connection connection) throws SQLException {
        ResultSet resultSet = connection.createStatement().executeQuery("SELECT setting FROM pg_settings WHERE name = '" + PostgresConstants.HBA_FILE_SETTING_NAME + "'");
        resultSet.next();

        return resultSet.getString(1);
    }

    private String getPgFacadePostgresqlConfFilePath(Connection connection) throws SQLException {
        ResultSet resultSet = connection.createStatement().executeQuery("SELECT setting FROM pg_settings WHERE name = '" + PostgresConstants.DATA_DIRECTORY_SETTING_NAME + "'");
        resultSet.next();

        return resultSet.getString(1) + "/" + PostgresqlConfConstants.PG_FACADE_POSTGRESQL_CONF_FILE_NAME;
    }

    private double extractPostgresVersion(Connection connection) throws SQLException {
        ResultSet resultSet = connection.createStatement().executeQuery("show server_version");
        resultSet.next();
        String showVersionResult = resultSet.getString(1);
        Matcher matcher = PostgresConstants.SHOW_SERVER_VERSION_PATTERN.matcher(showVersionResult);
        matcher.matches();

        return Double.parseDouble(matcher.group(1).trim());
    }
}
