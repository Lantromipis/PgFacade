package com.lantromipis.pgfacadeprotocol.message;

import com.lantromipis.pgfacadeprotocol.constant.MessageMarkerConstants;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class AppendResponse extends AbstractMessage {

    private long term;
    private boolean success;
    private long matchIndex;
    private long commitIndex;

    @Override
    public byte getMessageMarker() {
        return MessageMarkerConstants.APPEND_RESPONSE_MESSAGE_MARKER;
    }
}
