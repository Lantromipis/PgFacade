package com.lantromipis.pgfacadeprotocol.model.api;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RaftPeerInfo {
    private String groupId;
    private String id;
    private String ipAddress;
    private int port;
    private long lastTimeActive;
}
