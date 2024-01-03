package com.lantromipis.orchestration.service.api;

public interface LoadBalancerOrchestrator {
    void startOrchestration();

    void stopOrchestration();

    void shutdownLoadBalancer();
}
