package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.InitializationException;
import com.lantromipis.orchestration.exception.RaftException;
import com.lantromipis.orchestration.model.PgFacadeRaftNodeInfo;

public interface PgFacadeRaftService {
    void initialize() throws InitializationException;

    void addNewRaftNode(PgFacadeRaftNodeInfo newNodeInfo) throws RaftException;
}
