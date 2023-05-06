package com.lantromipis.connectionpool.model.stats;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionPoolStats {
    private int primaryPoolConnectionsLimit;
    private int standbyPoolConnectionsLimit;
    private int primaryPoolAllConnectionsCount;
    private int standbyPoolAllConnectionsCount;
    private int primaryPoolFreeConnectionsCount;
    private int standbyPoolFreeConnectionsCount;

}
