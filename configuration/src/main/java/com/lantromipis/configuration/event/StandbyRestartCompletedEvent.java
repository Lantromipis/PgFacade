package com.lantromipis.configuration.event;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.UUID;

/**
 * Event which is fired when standby restart completed. StandbyRestartStartedEvent must be fired before this event.
 */
@Data
@AllArgsConstructor
public class StandbyRestartCompletedEvent {
    /**
     * UUID of event. Same as in StandbyRestartStartedEvent which is fired before this event.
     */
    private UUID restartEventId;

    /**
     * Instance id of standby.
     */
    private UUID instanceId;

    /**
     * True if standby restarted successfully.
     */
    boolean success;
}
