package com.lantromipis.pgfacadeprotocol.model.api;

import lombok.*;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RaftServerProperties {
    private int port = 31000;
    private int startupLeaderHeartbeatAwait = 2000;
    private int voteTimeout = 2000;
    private int heartbeatTimeout = 100;
    private int aquireConnectionTimeout = 100;
}
