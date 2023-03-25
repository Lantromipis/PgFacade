package com.lantromipis.pgfacadeprotocol.message;

import com.lantromipis.pgfacadeprotocol.constant.MessageMarkerConstants;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class HelloRequest extends AbstractMessage {

    @Override
    public byte getMessageMarker() {
        return MessageMarkerConstants.HELLO_REQUEST_MESSAGE_MARKER;
    }
}
