package com.lantromipis.postgresprotocol.model.internal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageInfo {
    private byte startByte;
    private int length;
    private byte[] entireMessage;
}
