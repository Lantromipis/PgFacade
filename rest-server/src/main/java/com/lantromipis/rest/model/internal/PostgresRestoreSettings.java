package com.lantromipis.rest.model.internal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostgresRestoreSettings {
    private boolean saveAsNewPrimary;
}
