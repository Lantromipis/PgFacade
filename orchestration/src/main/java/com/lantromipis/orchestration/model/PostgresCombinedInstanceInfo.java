package com.lantromipis.orchestration.model;

import com.lantromipis.configuration.model.PostgresPersistedInstanceInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostgresCombinedInstanceInfo {
    private PostgresAdapterInstanceInfo adapter;
    private PostgresPersistedInstanceInfo persisted;
}
