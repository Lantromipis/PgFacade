package com.lantromipis.configuration.properties.predefined;

import io.smallrye.config.ConfigMapping;

import java.time.Duration;

@ConfigMapping(prefix = "pg-facade.raft")
public interface RaftProperties {
    int nodesCount();

    Duration nodesCheckInterval();

    int appChecksRetryBeforeKill();

    Duration raftNoResponseTimeoutBeforeKill();
}
