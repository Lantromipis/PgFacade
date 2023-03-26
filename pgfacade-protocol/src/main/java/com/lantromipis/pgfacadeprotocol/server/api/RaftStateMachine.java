package com.lantromipis.pgfacadeprotocol.server.api;

public interface RaftStateMachine {
    void operationCommitted(long commitIndex, String command, byte[] data);
}
