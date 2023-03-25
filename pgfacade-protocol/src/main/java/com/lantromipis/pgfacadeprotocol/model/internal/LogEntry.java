package com.lantromipis.pgfacadeprotocol.model.internal;

import lombok.Data;

@Data
public class LogEntry {
    private long term;
    private long index;
    private String command;
    private byte[] data;
}
