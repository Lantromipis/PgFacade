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
public class AppendRequest extends AbstractMessage {

    private long currentTerm;
    private long previousLogIndex;
    private long previousTerm;
    private long leaderCommit;
    private List<Operation> operations;
    private long shrinkIndex;

    @Getter
    @Builder
    public static class Operation {
        private long term;
        private long index;
        private String command;
        private byte[] data;
    }

    @Override
    public byte getMessageMarker() {
        return MessageMarkerConstants.APPEND_REQUEST_MESSAGE_MARKER;
    }
}
