package com.lantromipis.configuration.event;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.UUID;

/**
 * Event which is fired when standby is restart is started. StandbyRestartCompletedEvent must be fired after this event.
 */
@Data
@AllArgsConstructor
public class StandbyRestartStartedEvent {
    /**
     * UUID of event. Same as in StandbyRestartCompletedEvent which is fired before this event.
     */
    private UUID restartEventId;
    /**
     * Instance id of standby.
     */
    private UUID instanceId;

    /**
     * IP or host name of standby.
     */
    private String address;
}
