package com.lantromipis.postgresprotocol.model.internal;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Deque;

@Data
@AllArgsConstructor
public class PgChannelAuthResult {
    boolean success;
    private Deque<PgMessageInfo> serverStartMessagesInfos;

    public PgChannelAuthResult(boolean success) {
        this.success = success;
    }
}
