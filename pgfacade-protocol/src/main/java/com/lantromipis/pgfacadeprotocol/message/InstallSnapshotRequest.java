package com.lantromipis.pgfacadeprotocol.message;

import com.lantromipis.pgfacadeprotocol.constant.MessageMarkerConstants;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class InstallSnapshotRequest extends AbstractMessage {

    // modified than in original Raft paper for simplicity
    private long term;
    private String leaderId;
    private long lastIncludedIndex;
    private long leaderCommit;
    private List<ChunkData> chunks;

    @Getter
    @Builder
    public static class ChunkData {
        public String chunkName;
        public byte[] data;
    }

    @Override
    public byte getMessageMarker() {
        return MessageMarkerConstants.INSTALL_SNAPSHOT_REQUEST_MESSAGE_MARKER;
    }
}
