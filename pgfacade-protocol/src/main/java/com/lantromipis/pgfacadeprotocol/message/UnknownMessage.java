package com.lantromipis.pgfacadeprotocol.message;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class UnknownMessage extends AbstractMessage {
    @Override
    public byte getMessageMarker() {
        return 0;
    }
}
