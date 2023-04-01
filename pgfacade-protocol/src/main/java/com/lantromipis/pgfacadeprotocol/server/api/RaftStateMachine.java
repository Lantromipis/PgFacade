package com.lantromipis.pgfacadeprotocol.server.api;

import com.lantromipis.pgfacadeprotocol.model.api.SnapshotChunk;

import java.util.List;
import java.util.function.Consumer;

public interface RaftStateMachine {
    void operationCommitted(long commitIndex, String command, byte[] data);

    void takeSnapshot(long commitIndex, Consumer<SnapshotChunk> snapshotChunkConsumer);

    void installSnapshot(long commitIndex, List<SnapshotChunk> snapshotChunks);
}
