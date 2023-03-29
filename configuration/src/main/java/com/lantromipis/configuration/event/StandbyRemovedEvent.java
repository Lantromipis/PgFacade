package com.lantromipis.configuration.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Event which is fired when standby is removed.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class StandbyRemovedEvent {
    /**
     * Instance id of standby.
     */
    private UUID instanceId;
}
