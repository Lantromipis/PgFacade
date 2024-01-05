package com.lantromipis.rest.mapper;

import com.lantromipis.orchestration.model.PostgresClusterSettingsChangeResult;
import com.lantromipis.rest.model.api.postgres.PatchPostgresSettingsResponseDto;
import org.mapstruct.Mapper;

@Mapper
public abstract class PostgresSettingsMapper {
    public abstract PatchPostgresSettingsResponseDto from(PostgresClusterSettingsChangeResult source);
}
