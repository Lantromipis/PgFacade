package com.lantromipis.configuration.properties.predefined;

import io.smallrye.config.ConfigMapping;

import java.time.Duration;

@ConfigMapping(prefix = "pg-facade.raft")
public interface RaftProperties {

    int serverWorkThreads();

    int nodesCount();

    Duration nodesCheckInterval();

    int appChecksRetryBeforeKill();

    Duration raftNoResponseTimeoutBeforeKill();

    Duration commitTimeout();

    FollowerStartupHealthcheckProperties followerStartupHealthcheck();

    interface FollowerStartupHealthcheckProperties {
        int intervalMs();

        Duration timeout();
    }
}
