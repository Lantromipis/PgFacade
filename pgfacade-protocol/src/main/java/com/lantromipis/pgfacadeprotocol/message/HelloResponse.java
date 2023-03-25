package com.lantromipis.pgfacadeprotocol.message;

import com.lantromipis.pgfacadeprotocol.constant.MessageMarkerConstants;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class HelloResponse extends AbstractMessage {
    private boolean ack;
    private String currentLeaderNodeId;

    @Override
    public byte getMessageMarker() {
        return MessageMarkerConstants.HELLO_RESPONSE_MESSAGE_MARKER;
    }
}
