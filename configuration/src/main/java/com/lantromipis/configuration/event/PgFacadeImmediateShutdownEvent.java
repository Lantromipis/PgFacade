package com.lantromipis.configuration.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Fired in exceptional situations when it is better just to kill PgFacade because recovery is not possible
 */
@Data
@NoArgsConstructor
public class PgFacadeImmediateShutdownEvent {
}
