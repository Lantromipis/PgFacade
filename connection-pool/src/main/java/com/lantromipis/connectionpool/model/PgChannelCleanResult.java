package com.lantromipis.connectionpool.model;

import lombok.Getter;

@Getter
public class PgChannelCleanResult {
    private boolean success;

    public PgChannelCleanResult(boolean success) {
        this.success = success;
    }
}
