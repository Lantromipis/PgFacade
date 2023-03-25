package com.lantromipis.pgfacadeprotocol.message;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
@EqualsAndHashCode
@AllArgsConstructor
public abstract class AbstractMessage {
    @NonNull
    private String groupId;
    @NonNull
    private String nodeId;

    public abstract byte getMessageMarker();
}
