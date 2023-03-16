package com.lantromipis.connectionpool.model;

import com.lantromipis.postgresprotocol.model.internal.MessageInfo;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class PgChannelAuthResult {
    boolean success;
    private List<MessageInfo> serverStartMessagesInfos;

    public PgChannelAuthResult(boolean success) {
        this.success = success;
    }
}
