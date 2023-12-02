package com.lantromipis.pgfacadeprotocol.model.api;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RaftServerProperties {
    @Builder.Default
    private int port = 31000;
    @Builder.Default
    private int startupLeaderHeartbeatAwait = 2000;
    @Builder.Default
    private int voteTimeout = 2000;
    @Builder.Default
    private int syncMembershipTimeout = 60000;
    @Builder.Default
    private int electionRestartTimeoutMin = 1000;
    @Builder.Default
    private int electionRestartTimeoutMax = 4000;
    @Builder.Default
    private int heartbeatTimeout = 100;
    @Builder.Default
    private int aquireConnectionTimeout = 100;
    @Builder.Default
    private int shrinkLogEveryNumOfCommits = 10;
}
