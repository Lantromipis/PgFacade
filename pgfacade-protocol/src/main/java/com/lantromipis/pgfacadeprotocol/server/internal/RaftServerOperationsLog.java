package com.lantromipis.pgfacadeprotocol.server.internal;

import com.lantromipis.pgfacadeprotocol.model.internal.LogEntry;
import lombok.*;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

@Data
@Builder
@AllArgsConstructor
public class RaftServerOperationsLog {
    @Setter(AccessLevel.NONE)
    private ConcurrentMap<Long, LogEntry> operationsLog;
    private AtomicLong lastIndex;

    public RaftServerOperationsLog() {
        this.operationsLog = new ConcurrentHashMap<>();
        this.lastIndex = new AtomicLong(0);
    }

    public LogEntry getLogEntry(long index) {
        return operationsLog.get(index);
    }

    public long append(long term, String command, byte[] data) {
        long index = lastIndex.incrementAndGet();

        LogEntry logEntry = LogEntry
                .builder()
                .term(term)
                .command(command)
                .data(Arrays.copyOf(data, data.length))
                .index(index)
                .build();

        operationsLog.put(index, logEntry);

        return index;
    }

    public long getLastTerm() {
        LogEntry logEntry = operationsLog.get(lastIndex.get());

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
        for (long i = index; i <= lastIndex.get(); i++) {
            operationsLog.remove(i);
        }
    }
}
