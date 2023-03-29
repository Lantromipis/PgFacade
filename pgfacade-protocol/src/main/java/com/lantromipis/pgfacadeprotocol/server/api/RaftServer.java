package com.lantromipis.pgfacadeprotocol.server.api;

import com.lantromipis.pgfacadeprotocol.exception.NotActiveException;
import com.lantromipis.pgfacadeprotocol.exception.NotLeaderException;
import com.lantromipis.pgfacadeprotocol.model.api.RaftNode;

import java.util.concurrent.CountDownLatch;

public interface RaftServer {
    void start() throws InterruptedException;

    void shutdown();

    long appendToLog(String command, byte[] data) throws NotLeaderException, NotActiveException;

    void addNewNode(RaftNode raftNode) throws NotLeaderException, NotActiveException;
}
