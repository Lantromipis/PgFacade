package com.lantromipis.pgfacadeprotocol.model.api;

import lombok.*;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RaftServerProperties {
    private int port = 31000;
    private int startupLeaderHeartbeatAwait = 1000;
    private int voteTimeout = 1000;
    private int heartbeatTimeout = 500;
    private int aquireConnectionTimeout = 100;
}
