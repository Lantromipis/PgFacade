package com.lantromipis.orchestration.orchestrator.api;

public interface LoadBalancerOrchestrator {
    void startOrchestration();

    void stopOrchestration();

    void shutdownLoadBalancer();
}
