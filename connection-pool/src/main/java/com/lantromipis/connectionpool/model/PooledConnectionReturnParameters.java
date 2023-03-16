package com.lantromipis.connectionpool.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PooledConnectionReturnParameters {
    boolean rollback;
}
