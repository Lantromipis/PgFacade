package com.lantromipis.pgfacadeprotocol.message;

import com.lantromipis.pgfacadeprotocol.constant.MessageMarkerConstants;
import lombok.*;
import lombok.experimental.SuperBuilder;


@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class VoteRequest extends AbstractMessage {
    private long term;
    private long lastLogIndex;
    private long lastLogTerm;
    private int round;

    @Override
    public byte getMessageMarker() {
        return MessageMarkerConstants.VOTE_REQUEST_MESSAGE_MARKER;
    }
}
