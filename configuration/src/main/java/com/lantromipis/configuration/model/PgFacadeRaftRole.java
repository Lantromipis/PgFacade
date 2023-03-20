package com.lantromipis.configuration.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum PgFacadeRaftRole {
    LEADER("Leader"),
    FOLLOWER("Follower"),
    RAFT_DISABLED("Raft NA");

    @Getter
    private final String mdcValue;
}
