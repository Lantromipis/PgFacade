package com.lantromipis.orchestration.model.raft;

import com.lantromipis.orchestration.model.PostgresCombinedInstanceInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostgresSwitchoverCompletedNotification {
    private UUID notificationId;
    private boolean success;
    private Set<UUID> instanceToRemoveIds;
    private PostgresCombinedInstanceInfo newPrimaryCombinedInfo;
}
