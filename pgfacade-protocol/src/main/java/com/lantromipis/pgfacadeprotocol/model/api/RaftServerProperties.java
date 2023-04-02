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
    private int port = 31000;
    private int startupLeaderHeartbeatAwait = 2000;
    private int voteTimeout = 2000;
    private int syncMembershipTimeout = 60000;
    private int electionRestartTimeoutMin = 1000;
    private int electionRestartTimeoutMax = 4000;
    private int heartbeatTimeout = 100;
    private int aquireConnectionTimeout = 100;
    private int shrinkLogEveryNumOfCommits = 10;
}
