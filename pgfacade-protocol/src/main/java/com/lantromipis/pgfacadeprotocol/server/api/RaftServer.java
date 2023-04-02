package com.lantromipis.pgfacadeprotocol.server.api;

import com.lantromipis.pgfacadeprotocol.exception.NotActiveException;
import com.lantromipis.pgfacadeprotocol.exception.NotLeaderException;
import com.lantromipis.pgfacadeprotocol.model.api.RaftNode;
import com.lantromipis.pgfacadeprotocol.model.api.RaftPeerInfo;

import java.util.List;

public interface RaftServer {
    void start() throws InterruptedException;

    void shutdown();

    long appendToLog(String command, byte[] data) throws NotLeaderException, NotActiveException;

    void addNewNode(RaftNode raftNode) throws NotLeaderException, NotActiveException;

    void removeNode(String nodeId) throws NotLeaderException, NotActiveException;

    List<RaftPeerInfo> getRaftPeers();
}
