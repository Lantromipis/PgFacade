package com.lantromipis.pgfacadeprotocol.model.internal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogEntry {
    private long term;
    private long index;
    private String command;
    private byte[] data;
}
