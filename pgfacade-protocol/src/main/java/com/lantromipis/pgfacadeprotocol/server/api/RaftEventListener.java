package com.lantromipis.pgfacadeprotocol.server.api;

public interface RaftEventListener {
    void selfBecameLeader();
}
