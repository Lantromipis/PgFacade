package com.lantromipis.postgresprotocol.model.internal;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PgMessageInfo {
    private byte startByte;
    private ByteBuf entireMessage;

    public int getLength() {
        return entireMessage.readableBytes() - 1;
    }
}
