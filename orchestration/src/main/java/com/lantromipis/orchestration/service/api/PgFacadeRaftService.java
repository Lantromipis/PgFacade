package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.InitializationException;
import com.lantromipis.orchestration.exception.RaftException;
import com.lantromipis.orchestration.model.PgFacadeRaftNodeInfo;
import com.lantromipis.pgfacadeprotocol.model.api.RaftPeerInfo;

import java.util.List;

public interface PgFacadeRaftService {
    void initialize() throws InitializationException;

    void shutdown(boolean simulateRoleChangedToFollower);

    void addNewRaftNode(PgFacadeRaftNodeInfo newNodeInfo) throws RaftException;

    void removeNode(String nodeAdapterIdentifier) throws RaftException;

    List<RaftPeerInfo> getRaftPeersFromServer() throws RaftException;

    void appendToLogAndAwaitCommit(String command, byte[] data, long timeout) throws RaftException;
}
