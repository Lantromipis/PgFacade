package com.lantromipis.pgfacadeprotocol.server.api;

import com.lantromipis.pgfacadeprotocol.exception.NotActiveException;
import com.lantromipis.pgfacadeprotocol.exception.NotLeaderException;
import com.lantromipis.pgfacadeprotocol.model.api.RaftNode;

public interface RaftServer {
    void start() throws InterruptedException;

    void shutdown() throws InterruptedException;

    long appendToLog(String command, byte[] data) throws NotLeaderException, NotActiveException;

    void addNewNode(RaftNode raftNode) throws NotLeaderException, NotActiveException;
}
