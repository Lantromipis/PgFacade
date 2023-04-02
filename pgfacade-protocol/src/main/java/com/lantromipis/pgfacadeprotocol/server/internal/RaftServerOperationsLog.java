package com.lantromipis.pgfacadeprotocol.server.internal;

import com.lantromipis.pgfacadeprotocol.model.internal.LogEntry;
import com.lantromipis.pgfacadeprotocol.utils.RaftUtils;
import lombok.*;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Data
@Builder
@AllArgsConstructor
public class RaftServerOperationsLog {
    @Setter(AccessLevel.NONE)
    private ConcurrentHashMap<Long, LogEntry> operationsLog;
    private AtomicLong effectiveLastIndex;
    @Setter(AccessLevel.NONE)
    private AtomicLong realLastIndex;
    private AtomicLong firstIndexInLog;
    private AtomicLong commitsFromLastShrink;

    public RaftServerOperationsLog() {
        this.operationsLog = new ConcurrentHashMap<>();
        this.effectiveLastIndex = new AtomicLong(-1);
        this.realLastIndex = new AtomicLong(-1);
        this.firstIndexInLog = new AtomicLong(0);
        this.commitsFromLastShrink = new AtomicLong(0);
    }

    public LogEntry getLogEntry(long index) {
        return operationsLog.get(index);
    }

    public long append(long term, String command, byte[] data) {
        LogEntry logEntry = LogEntry
                .builder()
                .term(term)
                .command(command)
                .data(Arrays.copyOf(data, data.length))
                .build();

        long index = realLastIndex.incrementAndGet();

        operationsLog.put(index, logEntry);
        logEntry.setIndex(index);
        RaftUtils.updateIncrementalAtomicLong(effectiveLastIndex, index);

        return index;
    }

    public long getLastTerm() {
        LogEntry logEntry = operationsLog.get(effectiveLastIndex.get());

        if (logEntry == null) {
            return 0;
        }

        return logEntry.getTerm();
    }

    public long getTerm(long index) {
        LogEntry logEntry = operationsLog.get(index);

        if (logEntry == null) {
            return 0;
        }

        return logEntry.getTerm();
    }

    public void removeAllStartingFrom(long index) {
        for (long i = index; i <= effectiveLastIndex.get(); i++) {
            operationsLog.remove(i);
        }
        RaftUtils.updateIncrementalAtomicLong(firstIndexInLog, index + 1);
    }

    public void shrinkLog(long lastIndexToLeave) {
        for (Long key : operationsLog.keySet()) {
            if (key < lastIndexToLeave) {
                operationsLog.remove(key);
            }
        }
        RaftUtils.updateIncrementalAtomicLong(firstIndexInLog, lastIndexToLeave);
        RaftUtils.updateIncrementalAtomicLong(effectiveLastIndex, lastIndexToLeave);
        RaftUtils.updateIncrementalAtomicLong(realLastIndex, lastIndexToLeave);
    }

    public boolean containsIndex(long index) {
        return operationsLog.containsKey(index);
    }
}
