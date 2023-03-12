package com.lantromipis.orchestration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.file.WatchService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Data
@Builder
@AllArgsConstructor
public class ArchiverWalStreamingState {
    private AtomicBoolean watchServiceActive;
    private AtomicBoolean processActive;
    private WatchService walDirWatcherService;
    private Process pgReceiveWallProcess;
    private String pgReceiveWalStdErr;
    private AtomicInteger unsuccessfulRetries;
    private AtomicBoolean switchoverActionProcessed;

    public ArchiverWalStreamingState() {
        watchServiceActive = new AtomicBoolean(false);
        processActive = new AtomicBoolean(false);
        unsuccessfulRetries = new AtomicInteger(0);
        switchoverActionProcessed = new AtomicBoolean(false);
    }
}
