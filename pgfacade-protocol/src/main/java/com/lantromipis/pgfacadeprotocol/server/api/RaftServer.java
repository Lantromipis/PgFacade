package com.lantromipis.pgfacadeprotocol.server.api;

import com.lantromipis.pgfacadeprotocol.model.api.RaftNode;

public interface RaftServer {
    void start() throws InterruptedException;

    void addNewNode(RaftNode raftNode);
}
