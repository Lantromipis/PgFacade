package com.lantromipis.orchestration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Connection;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SelectedForPromotionStandby {
    private PostgresCombinedInstanceInfo standbyInfo;
    private Connection connection;
}
