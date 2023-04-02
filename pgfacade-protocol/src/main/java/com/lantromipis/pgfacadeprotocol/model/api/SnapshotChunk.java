package com.lantromipis.pgfacadeprotocol.model.api;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SnapshotChunk {
    private String name;
    private byte[] data;
}
