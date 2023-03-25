package com.lantromipis.pgfacadeprotocol.message;

import com.lantromipis.pgfacadeprotocol.constant.MessageMarkerConstants;
import lombok.*;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class VoteResponse extends AbstractMessage {
    private long term;
    private boolean agreed;
    private int round;

    @Override
    public byte getMessageMarker() {
        return MessageMarkerConstants.VOTE_RESPONSE_MESSAGE_MARKER;
    }
}
