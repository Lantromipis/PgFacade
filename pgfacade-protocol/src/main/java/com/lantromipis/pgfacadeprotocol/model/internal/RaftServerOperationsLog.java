package com.lantromipis.pgfacadeprotocol.model.internal;

import lombok.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

@Data
@Builder
@AllArgsConstructor
public class RaftServerOperationsLog {
    private ConcurrentMap<Long, LogEntry> operationsLog;
    private AtomicLong lastIndex;
    private AtomicLong lastTerm;

    public RaftServerOperationsLog() {
        this.operationsLog = new ConcurrentHashMap<>();
        this.lastIndex = new AtomicLong(0);
        this.lastTerm = new AtomicLong(0);
    }

    public long getTerm(long index) {
        LogEntry logEntry = operationsLog.get(index);

        if (logEntry == null) {
            return 0;
        }

        return logEntry.getTerm();
    }
}
