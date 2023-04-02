package com.lantromipis.pgfacadeprotocol.model.internal;

import com.lantromipis.pgfacadeprotocol.message.VoteResponse;
import com.lantromipis.pgfacadeprotocol.model.api.RaftNode;
import com.lantromipis.pgfacadeprotocol.model.api.RaftRole;
import com.lantromipis.pgfacadeprotocol.server.api.RaftEventListener;
import com.lantromipis.pgfacadeprotocol.server.api.RaftStateMachine;
import com.lantromipis.pgfacadeprotocol.server.impl.RaftTimer;
import com.lantromipis.pgfacadeprotocol.server.internal.RaftServerOperationsLog;
import io.netty.channel.EventLoopGroup;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Data
@Builder
@AllArgsConstructor
public class RaftServerContext {

    private RaftTimer heartbeatTimer;
    private RaftTimer voteTimer;

    private String workDirPath;

    private boolean active;
    private AtomicBoolean commitInProgress;

    // netty
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    // general raft info
    private String raftGroupId;
    private ConcurrentHashMap<String, RaftPeerWrapper> raftPeers;

    // self info
    private String selfNodeId;
    private RaftNode selfNode;
    private RaftRole selfRole = RaftRole.FOLLOWER;
    private AtomicLong currentTerm;
    private AtomicLong commitIndex;
    private AtomicLong stateMachineApplyIndex;
    private AtomicBoolean notifiedStartupSync;

    private RaftServerOperationsLog operationLog;

    // election
    private String votedForNodeId;
    private Map<String, VoteResponse> voteResponses;

    private RaftEventListener eventListener;
    private RaftStateMachine raftStateMachine;

    public RaftServerContext() {
        currentTerm = new AtomicLong(0);
        commitIndex = new AtomicLong(-1);
        stateMachineApplyIndex = new AtomicLong(-1);
        notifiedStartupSync = new AtomicBoolean(false);
        commitInProgress = new AtomicBoolean(false);
        voteResponses = new ConcurrentHashMap<>();
        raftPeers = new ConcurrentHashMap<>();

        active = false;
    }

    public void setSelfRole(RaftRole newRole) {
        RaftRole prevRole = this.selfRole;
        this.selfRole = newRole;

        if (!prevRole.equals(selfRole) && eventListener != null) {
            eventListener.selfRoleChanged(newRole);
        }

        if (!notifiedStartupSync.get() && RaftRole.LEADER.equals(newRole) && eventListener != null && notifiedStartupSync.compareAndSet(false, true)) {
            eventListener.syncedWithLeaderOrSelfIsLeaderOnStartup();
        }
    }
}
