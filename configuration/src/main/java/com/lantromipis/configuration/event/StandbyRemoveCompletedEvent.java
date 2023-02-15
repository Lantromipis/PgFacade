package com.lantromipis.configuration.event;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.UUID;

/**
 * Event which is fired when switchover completed. StandbyRemoveStartedEvent must be fired before this event.
 */
@Data
@AllArgsConstructor
public class StandbyRemoveCompletedEvent {
    /**
     * UUID of event. Same as in StandbyRemoveStartedEvent which is fired before this event.
     */
    private UUID removerEventId;

    /**
     * Instance id of standby.
     */
    private UUID instanceId;

    /**
     * IP or host name of standby.
     */
    private String address;

    /**
     * True if standby removed successfully.
     */
    boolean success;
}
