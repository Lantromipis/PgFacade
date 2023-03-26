package com.lantromipis.orchestration.service.impl.raft;

import com.lantromipis.pgfacadeprotocol.server.api.RaftStateMachine;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;

@Slf4j
@ApplicationScoped
public class RaftStateMachineImpl implements RaftStateMachine {
    @Override
    public void operationCommitted(long commitIndex, String command, byte[] data) {
        log.info("State machine commit! Commit: {} Command: {} data: {}", commitIndex, command, new String(data));
    }
}
