package com.lantromipis.pgfacadeprotocol.message;

import com.lantromipis.pgfacadeprotocol.constant.MessageMarkerConstants;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class InstallSnapshotResponse extends AbstractMessage {

    private long term;
    private boolean success;
    private long lastIndex;

    @Override
    public byte getMessageMarker() {
        return MessageMarkerConstants.INSTALL_SNAPSHOT_RESPONSE_MESSAGE_MARKER;
    }
}
