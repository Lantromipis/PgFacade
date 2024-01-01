package com.lantromipis.orchestration.model.raft;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostgresPersistedArchiverInfo {
    private String lastUploadedWal;
    // to guarantee full restart of stream after pg_resetwal
    private long walSegmentSizeInBytes;
}
