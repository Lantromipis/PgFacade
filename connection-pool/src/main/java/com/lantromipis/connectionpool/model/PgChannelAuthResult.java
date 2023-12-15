package com.lantromipis.connectionpool.model;

import com.lantromipis.postgresprotocol.model.internal.MessageInfo;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Deque;

@Data
@AllArgsConstructor
public class PgChannelAuthResult {
    boolean success;
    private Deque<MessageInfo> serverStartMessagesInfos;

    public PgChannelAuthResult(boolean success) {
        this.success = success;
    }
}
