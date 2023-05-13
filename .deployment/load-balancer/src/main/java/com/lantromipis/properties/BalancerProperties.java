package com.lantromipis.properties;

import io.smallrye.config.ConfigMapping;

import java.time.Duration;

@ConfigMapping(prefix = "balancer")
public interface BalancerProperties {
    int primaryPort();
    int standbyPort();

    BalancingAlgorithm algorithm();

    Duration hostsRefreshInterval();

    enum BalancingAlgorithm {
        ROUND_ROBIN
    }
}
