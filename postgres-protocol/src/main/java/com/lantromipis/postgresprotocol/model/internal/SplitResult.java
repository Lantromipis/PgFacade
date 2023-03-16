package com.lantromipis.postgresprotocol.model.internal;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SplitResult {
    private List<MessageInfo> messageInfos;
    private ByteBuf lastIncompleteMessage;
}
