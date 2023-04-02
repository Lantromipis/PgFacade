package com.lantromipis.pgfacadeprotocol.message;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
@EqualsAndHashCode
@AllArgsConstructor
public abstract class AbstractMessage {
    private String groupId;
    private String nodeId;

    public abstract byte getMessageMarker();
}
