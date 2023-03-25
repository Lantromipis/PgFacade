package com.lantromipis.pgfacadeprotocol.model.internal;

import com.lantromipis.pgfacadeprotocol.message.VoteResponse;
import com.lantromipis.pgfacadeprotocol.model.api.RaftRole;
import com.lantromipis.pgfacadeprotocol.server.api.RaftEventListener;
import io.netty.channel.EventLoopGroup;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RaftServerContext {
    // netty
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    // general raft info
    private String raftGroupId;
    private Map<String, RaftPeerWrapper> raftPeers;

    // self info
    private String selfNodeId;
    private RaftRole selfRole;
    private AtomicLong currentTerm;
    private AtomicLong commitIndex;

    private RaftServerOperationsLog raftServerOperationsLog;

    // election
    private String votedForNodeId;
    private List<VoteResponse> voteResponses;

    private RaftEventListener raftEventListener;

    public void setSelfRole(RaftRole selfRole) {
        this.selfRole = selfRole;
        if (raftEventListener != null && selfRole.equals(RaftRole.LEADER)) {
            raftEventListener.selfBecameLeader();
        }
    }
}
