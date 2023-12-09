package com.lantromipis.connectionpool.model;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Getter
public class StorageAwaitRequest {
    /**
     * Used for synchronization. Caller set it to true when reached timeout and not waiting for connection. On the other hand storage sets it true before calling connectionReadyCallback.
     * Before calling connectionReadyCallback storage will check if this boolean is true. If so, then caller does not need connection anymore (ex. timeout reached).
     */
    private final AtomicBoolean synchronizationPoint;

    /**
     * Result of awaiting connection
     */
    @Setter
    private Consumer<PooledConnectionInternalInfo> connectionReadyCallback;

    public StorageAwaitRequest() {
        synchronizationPoint = new AtomicBoolean(false);
    }
}
