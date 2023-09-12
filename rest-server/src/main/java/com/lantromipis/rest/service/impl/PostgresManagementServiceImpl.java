package com.lantromipis.rest.service.impl;

import com.lantromipis.configuration.producers.RuntimePostgresConnectionProducer;
import com.lantromipis.orchestration.service.api.PostgresOrchestrator;
import com.lantromipis.rest.constant.PostgresConstants;
import com.lantromipis.rest.exception.GeneralRequestProcessingException;
import com.lantromipis.rest.model.api.postgres.PatchPostgresSettingsRequestDto;
import com.lantromipis.rest.model.api.postgres.PostgresSettingDescriptionDto;
import com.lantromipis.rest.model.api.postgres.PostgresSettingValueDto;
import com.lantromipis.rest.model.api.postgres.PostgresSettingsResponseDto;
import com.lantromipis.rest.service.api.PostgresManagementService;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ApplicationScoped
public class PostgresManagementServiceImpl implements PostgresManagementService {

    @Inject
    RuntimePostgresConnectionProducer runtimePostgresConnectionProducer;

    @Inject
    PostgresOrchestrator postgresOrchestrator;

    @Override
    public PostgresSettingsResponseDto getCurrentSettings() {
        try (Connection connection = runtimePostgresConnectionProducer.createNewPgFacadeUserConnectionToCurrentPrimary()) {
            ResultSet resultSet = connection.createStatement().executeQuery("SELECT name, setting, unit, category, vartype, short_desc, context, enumvals FROM pg_settings");

            List<PostgresSettingDescriptionDto> settingDescriptionDtos = new ArrayList<>();

            while (resultSet.next()) {
                settingDescriptionDtos.add(
                        PostgresSettingDescriptionDto
                                .builder()
                                .category(resultSet.getString("category"))
                                .description(resultSet.getString("short_desc"))
                                .value(resultSet.getString("setting"))
                                .unit(resultSet.getString("unit"))
                                .enumValues(resultSet.getString("enumvals"))
                                .type(resultSet.getString("vartype"))
                                .name(resultSet.getString("name"))
                                .context(resultSet.getString("context"))
                                .build()
                );
            }

            return PostgresSettingsResponseDto
                    .builder()
                    .importantNotes(PostgresConstants.IMPORTANT_NOTES)
                    .settingContextDescriptions(PostgresConstants.SETTING_CONTEXT_DESCRIPTIONS)
                    .currentSettings(settingDescriptionDtos)
                    .build();

        } catch (SQLException sqlException) {
            throw new GeneralRequestProcessingException("Error while executing SQL", sqlException);
        }
    }

    @Override
    public PostgresSettingsResponseDto patchSettings(PatchPostgresSettingsRequestDto requestDto) {
        Map<String, String> mapOfSettings = requestDto.getSettingsToPatch()
                .stream()
                .collect(Collectors.toMap(PostgresSettingValueDto::getName, PostgresSettingValueDto::getValue));

        postgresOrchestrator.changePostgresSettings(mapOfSettings);

        return getCurrentSettings();
    }
}
