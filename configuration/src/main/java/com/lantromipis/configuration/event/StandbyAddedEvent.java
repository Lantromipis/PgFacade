package com.lantromipis.configuration.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Event which is fired when new standby is added.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class StandbyAddedEvent {
    /**
     * Instance id of standby.
     */
    private UUID instanceId;
}
