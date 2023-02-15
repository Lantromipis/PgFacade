package com.lantromipis.configuration.event;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.UUID;

/**
 * Event which is fired when switchover completed. SwitchoverStartedEvent must be fired before this event.
 */
@Data
@AllArgsConstructor
public class SwitchoverCompletedEvent {
    /**
     * UUID of event. Same as in SwitchoverStartedEvent which is fired before this event.
     */
    private UUID switchoverEventId;
    /**
     * Indicates if switchover was successful
     */
    boolean success;
}
