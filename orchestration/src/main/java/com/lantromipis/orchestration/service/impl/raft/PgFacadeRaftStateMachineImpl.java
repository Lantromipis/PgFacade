package com.lantromipis.orchestration.service.impl.raft;

import com.lantromipis.orchestration.exception.RaftException;
import com.lantromipis.orchestration.service.api.PgFacadeRaftStateMachine;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@ApplicationScoped
public class PgFacadeRaftStateMachineImpl implements PgFacadeRaftStateMachine {

    private ConcurrentMap<Long, CountDownLatch> commitIndexLatches = new ConcurrentHashMap<>();

    private AtomicLong lastCommitIdx = new AtomicLong(-1);

    @Override
    public void operationCommitted(long commitIndex, String command, byte[] data) {
        log.info("State machine commit! Commit: {} Command: {} data: {}", commitIndex, command, new String(data));
        lastCommitIdx.set(commitIndex);
        Optional.ofNullable(commitIndexLatches.get(commitIndex)).ifPresent(CountDownLatch::countDown);
    }

    @Override
    public void awaitCommit(long operationIndex, long timeoutMs) throws InterruptedException, RaftException {
        try {
            CountDownLatch latch = new CountDownLatch(1);
            commitIndexLatches.put(operationIndex, latch);

            if (lastCommitIdx.get() >= operationIndex) {
                return;
            }

            if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                throw new RaftException("Timeout reached while waiting for operation to commit!");
            }
        } finally {
            commitIndexLatches.remove(operationIndex);
        }
    }
}
