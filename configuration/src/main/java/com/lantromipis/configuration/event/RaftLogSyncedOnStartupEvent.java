package com.lantromipis.configuration.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Fired when Raft log is synced with raft leader. For more details, {@link com.lantromipis.orchestration.service.impl.raft.RaftEventListenerImpl}
 */
@Data
@NoArgsConstructor
public class RaftLogSyncedOnStartupEvent {
}
