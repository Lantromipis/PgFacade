package com.lantromipis.postgresprotocol.model.internal;

import com.lantromipis.postgresprotocol.model.protocol.ErrorResponse;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Deque;

@Data
@AllArgsConstructor
public class PgChannelAuthResult {
    boolean success;
    private Deque<PgMessageInfo> serverStartMessagesInfos;
    private ErrorResponse errorResponse;

    public PgChannelAuthResult(boolean success) {
        this.success = success;
        this.errorResponse = null;
    }

    public PgChannelAuthResult(boolean success, Deque<PgMessageInfo> serverStartMessagesInfos) {
        this.success = success;
        this.serverStartMessagesInfos = serverStartMessagesInfos;
    }

    public PgChannelAuthResult(ErrorResponse errorResponse) {
        this.success = false;
        this.errorResponse = errorResponse;
    }
}
