package com.lantromipis.connectionpool.model;

import lombok.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

@Getter
public class StorageAwaitRequest {
    /**
     * Latch that caller will use to lock and await. Storage will count down it after setting awaitResult.
     */
    private CountDownLatch callerLatch;

    /**
     * Used for synchronization. Caller set it to true when reached timeout and not waiting for connection, and storage sets it true before setting awaitResult.
     * Before setting awaitResult storage will check if this boolean is true. If so, then caller does not need connection anymore.
     */
    private AtomicBoolean synchronizationPoint;

    /**
     * Result of awaiting connection
     */
    @Setter
    private PooledConnectionInternalInfo awaitResult;

    public StorageAwaitRequest() {
        callerLatch = new CountDownLatch(1);
        synchronizationPoint = new AtomicBoolean(false);
    }
}
