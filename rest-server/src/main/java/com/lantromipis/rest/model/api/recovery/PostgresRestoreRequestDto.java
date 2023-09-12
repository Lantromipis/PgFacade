package com.lantromipis.rest.model.api.recovery;

import lombok.Data;

@Data
public class PostgresRestoreRequestDto {
    private boolean saveRestoredInstanceAsNewPrimary;
}
