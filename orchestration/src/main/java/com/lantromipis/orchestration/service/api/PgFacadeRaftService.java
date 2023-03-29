package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.InitializationException;
import com.lantromipis.orchestration.exception.RaftException;
import com.lantromipis.orchestration.model.PgFacadeRaftNodeInfo;

public interface PgFacadeRaftService {
    void initialize() throws InitializationException;

    void shutdown();

    void addNewRaftNode(PgFacadeRaftNodeInfo newNodeInfo) throws RaftException;

    void appendToLogAndAwaitCommit(String command, byte[] data, long timeout) throws RaftException;
}
