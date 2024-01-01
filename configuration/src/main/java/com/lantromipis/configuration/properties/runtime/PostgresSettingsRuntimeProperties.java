package com.lantromipis.configuration.properties.runtime;

import com.lantromipis.configuration.event.PostgresSettingsUpdatedEvent;
import com.lantromipis.configuration.model.PgSetting;
import com.lantromipis.configuration.producers.RuntimePostgresConnectionProducer;
import com.lantromipis.configuration.properties.constant.PostgresConstants;
import com.lantromipis.configuration.utils.PostgresSettingsUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Getter
@ApplicationScoped
public class PostgresSettingsRuntimeProperties {

    @Inject
    RuntimePostgresConnectionProducer runtimePostgresConnectionProducer;

    @Inject
    Event<PostgresSettingsUpdatedEvent> postgresSettingsUpdatedEvent;

    private int postgresVersionNum = 150005;
    private int maxPostgresConnections = 100;
    private long walSegmentSizeInBytes = 16777216;

    public void reload() throws Exception {
        try (Connection connection = runtimePostgresConnectionProducer.createNewPgFacadeUserConnectionToCurrentPrimary()) {
            ResultSet pgSettingsResultSet = connection.createStatement().executeQuery("SELECT name, setting, context, unit FROM pg_settings");

            Map<String, PgSetting> settingNameToValue = new HashMap<>();

            while (pgSettingsResultSet.next()) {
                String settingName = pgSettingsResultSet.getString("name");
                String settingValue = pgSettingsResultSet.getString("setting");
                String settingUnit = pgSettingsResultSet.getString("unit");

                settingNameToValue.put(
                        settingName,
                        PgSetting
                                .builder()
                                .settingValue(settingValue)
                                .name(settingName)
                                .unit(settingUnit)
                                .build()
                );
            }

            // set connection limit
            int maxConnections = Integer.parseInt(settingNameToValue.get(PostgresConstants.MAX_CONNECTIONS_SETTING_NAME).getSettingValue());
            int superuserReservedConnections = Integer.parseInt(settingNameToValue.get(PostgresConstants.SUPERUSER_RESERVED_CONNECTIONS_SETTING_NAME).getSettingValue());
            maxPostgresConnections = maxConnections - superuserReservedConnections;

            // set version in format 150005
            postgresVersionNum = Integer.parseInt(settingNameToValue.get(PostgresConstants.SERVER_VERSION_NUM_SETTING_NAME).getSettingValue());

            // set wal segment size
            PgSetting walSegmentSizeSetting = settingNameToValue.get(PostgresConstants.WAL_SEGMENT_SIZE_SETTING_NAME);
            walSegmentSizeInBytes = PostgresSettingsUtils.convertPgMemoryValueToBytes(
                    Integer.parseInt(walSegmentSizeSetting.getSettingValue()),
                    walSegmentSizeSetting.getUnit()
            );

            postgresSettingsUpdatedEvent.fire(new PostgresSettingsUpdatedEvent());
        }
    }
}
