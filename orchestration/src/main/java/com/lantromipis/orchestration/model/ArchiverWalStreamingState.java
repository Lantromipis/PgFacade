package com.lantromipis.orchestration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.file.WatchService;
import java.util.concurrent.atomic.AtomicBoolean;

@Data
@Builder
@AllArgsConstructor
public class ArchiverWalStreamingState {
    private AtomicBoolean watchServiceActive;
    private AtomicBoolean processActive;
    private WatchService walDirWatcherService;
    private Process pgReceiveWallProcess;

    public ArchiverWalStreamingState() {
        watchServiceActive = new AtomicBoolean(false);
        processActive = new AtomicBoolean(false);
    }
}
