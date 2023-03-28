package com.lantromipis.orchestration.service.api;

import com.lantromipis.orchestration.exception.RaftException;
import com.lantromipis.pgfacadeprotocol.server.api.RaftStateMachine;

public interface PgFacadeRaftStateMachine extends RaftStateMachine {
    void awaitCommit(long operationIndex, long timeoutMs) throws InterruptedException, RaftException;
}
