package com.lantromipis.orchestration.service.api;

public interface PostgresContinuousArchivingService {
    boolean isContinuousArchivingActive();

    boolean startContinuousArchiving(boolean forceUseLatestServerLsn);

    void stopContinuousArchiving();
}
