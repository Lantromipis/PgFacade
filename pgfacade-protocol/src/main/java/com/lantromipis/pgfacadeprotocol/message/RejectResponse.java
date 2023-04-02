package com.lantromipis.pgfacadeprotocol.message;

import com.lantromipis.pgfacadeprotocol.constant.MessageMarkerConstants;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class RejectResponse extends AbstractMessage {

    private String message;

    @Override
    public byte getMessageMarker() {
        return MessageMarkerConstants.REJECT_MESSAGE_MARKER;
    }
}
