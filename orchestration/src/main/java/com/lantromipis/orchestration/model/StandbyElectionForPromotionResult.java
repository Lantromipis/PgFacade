package com.lantromipis.orchestration.model;

import com.lantromipis.orchestration.util.JdbcUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Connection;
import java.util.Map;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StandbyElectionForPromotionResult {
    private PostgresCombinedInstanceInfo electedForPromotionStandby;
    private Connection electedForPromotionStandbyConnection;
    private Map<UUID, PostgresCombinedInstanceInfo> otherStandbys;

    public void freeResources() {
        JdbcUtils.closeJdbcConnectionSafely(electedForPromotionStandbyConnection);
    }
}
