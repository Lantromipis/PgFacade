package com.lantromipis.configuration.event;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.UUID;

/**
 * Event which is fired when switchover started. SwitchoverCompletedEvent must be fired after this event.
 * During this event it is not guaranteed that any Postgres instance in cluster will work.
 */
@Data
@AllArgsConstructor
public class SwitchoverStartedEvent {
    /**
     * UUID of event. Same as in SwitchoverCompletedEvent which is fired after this event.
     */
    private UUID switchoverEventId;
}
