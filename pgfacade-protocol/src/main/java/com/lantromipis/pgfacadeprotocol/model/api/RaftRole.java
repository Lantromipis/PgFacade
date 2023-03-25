package com.lantromipis.pgfacadeprotocol.model.api;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum RaftRole {
    LEADER((byte) 0),
    FOLLOWER((byte) 1),
    CANDIDATE((byte) 2);

    @Getter
    private final byte protocolByte;
}
