package com.lantromipis.pgfacadeprotocol.model.internal;

import com.lantromipis.pgfacadeprotocol.message.VoteResponse;
import com.lantromipis.pgfacadeprotocol.model.api.RaftRole;
import com.lantromipis.pgfacadeprotocol.server.api.RaftEventListener;
import com.lantromipis.pgfacadeprotocol.server.api.RaftStateMachine;
import com.lantromipis.pgfacadeprotocol.server.internal.RaftServerOperationsLog;
import io.netty.channel.EventLoopGroup;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RaftServerContext {
    private boolean active;
    private AtomicBoolean commitInProgress;

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

    private RaftServerOperationsLog operationLog;

    // election
    private String votedForNodeId;
    private List<VoteResponse> voteResponses;

    private RaftEventListener eventListener;
    private RaftStateMachine raftStateMachine;

    public void setSelfRole(RaftRole newRole) {
        RaftRole prevRole = this.selfRole;
        this.selfRole = newRole;

        if (eventListener != null) {
            if (!prevRole.equals(RaftRole.LEADER) && newRole.equals(RaftRole.LEADER)) {
                eventListener.selfBecameLeader();
            } else if (prevRole.equals(RaftRole.LEADER) && newRole.equals(RaftRole.FOLLOWER)) {
                eventListener.selfBecomeFollower();
            }
        }
    }
}
