package com.lantromipis.configuration.event;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.UUID;

/**
 * Event which is fired when standby is removed. StandbyRemoveCompletedEvent must be fired after this event.
 */
@Data
@AllArgsConstructor
public class StandbyRemoveStartedEvent {
    /**
     * UUID of event. Same as in StandbyRemoveCompletedEvent which is fired after this event.
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
}
